import os
from argparse import ArgumentParser
from influxdb_client.client.influxdb_client import InfluxDBClient

BUCKET = "historical"


class MeasurementPoint:
    def __init__(self, time: str, value: float):
        self.time = time
        self.value = value


def get_stations(client: InfluxDBClient):
    query = (
        f'import "influxdata/influxdb/schema"\n'
        f'schema.tagValues(bucket: "{BUCKET}", tag: "station")'
    )
    result = client.query_api().query(query)
    stations = [record.get_value() for table in result for record in table.records]
    return stations


def get_measurements(client: InfluxDBClient):
    query = (
        f'import "influxdata/influxdb/schema"\nschema.measurements(bucket: "{BUCKET}")'
    )
    result = client.query_api().query(query)
    measurements = [record.get_value() for table in result for record in table.records]
    return measurements


def get_measurement_points(client: InfluxDBClient, station: str):
    query = (
        f'from(bucket: "{BUCKET}")\n'
        f"  |> range(start: -1y)\n"
        f'  |> filter(fn: (r) => r["station"] == "{station}")\n'
        f'  |> filter(fn: (r) => r["_field"] == "value")\n'
        f'  |> group(columns: ["_measurement", "station", "_field"])\n'
        f"  |> aggregateWindow(every: 1d, fn: mean, createEmpty: true)\n"
        f'  |> yield(name: "mean")'
    )
    result = client.query_api().query(query)

    # Create points, group by measurement
    points: dict[str, list[MeasurementPoint]] = {}
    for table in result:
        for record in table.records:
            point = MeasurementPoint(time=record.get_time(), value=record.get_value())
            measurement = record.get_measurement()
            if measurement not in points:
                points[measurement] = []
            points[measurement].append(point)
    return points


def main():
    parser = ArgumentParser(description="Extract data from InfluxDB")
    parser.add_argument("--output", type=str, help="Output file path", required=True)
    args = parser.parse_args()

    url = os.getenv("INFLUXDB_URL")
    token = os.getenv("INFLUXDB_TOKEN")
    org = os.getenv("INFLUXDB_ORG")

    if not url:
        raise EnvironmentError("INFLUXDB_URL not set in environment variables.")
    if not token:
        raise EnvironmentError("INFLUXDB_TOKEN not set in environment variables.")
    if not org:
        raise EnvironmentError("INFLUXDB_ORG not set in environment variables.")

    # Connect to InfluxDB and extract data
    client = InfluxDBClient(url=url, token=token, org=org)

    stations = get_stations(client)
    measurements = get_measurements(client)

    progress = 0
    total = len(stations)

    # Get measurements for each station
    for station in stations:
        csv_file_path = os.path.join(args.output, f"{station}_measurements.csv")

        with open(csv_file_path, "w") as csv_file:
            # Write CSV header
            header = "time," + ",".join(measurements) + "\n"
            csv_file.write(header)

            # Prepare data dictionary
            data_dict: dict[str, dict[str, float]] = {}
            for measurement in measurements:
                points = get_measurement_points(client, station).get(measurement, [])
                for point in points:
                    if point.time not in data_dict:
                        data_dict[point.time] = {}
                    data_dict[point.time][measurement] = point.value

            # Write data rows
            for time in sorted(data_dict.keys()):
                row = [str(time)]
                for measurement in measurements:
                    value = data_dict[time].get(measurement, None)
                    row.append(str(value) if value is not None else "")
                csv_file.write(",".join(row) + "\n")

        progress += 1
        print(f"Progress: {progress}/{total} stations processed.")


if __name__ == "__main__":
    main()
