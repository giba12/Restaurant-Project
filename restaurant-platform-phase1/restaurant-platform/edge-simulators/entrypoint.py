"""
Single entrypoint for all four simulator images. Which sensor type this
container instance runs is selected entirely by the SENSOR_TYPE env var,
set per-Deployment in the Helm chart. This keeps one Docker image for
"one container per simulated sensor type" instead of four near-duplicate
images to build/tag/import separately.
"""

import os
import sys

SENSOR_MODULES = {
    "plate-waste": "simulators.plate_waste",
    "pos-transaction": "simulators.pos_transaction",
    "service-timing": "simulators.service_timing",
    "staff-shift": "simulators.staff_shift",
}


def main():
    sensor_type = os.environ.get("SENSOR_TYPE")
    if sensor_type not in SENSOR_MODULES:
        print(
            f"FATAL: SENSOR_TYPE must be one of {list(SENSOR_MODULES)}, got {sensor_type!r}",
            file=sys.stderr,
        )
        sys.exit(1)

    module_name = SENSOR_MODULES[sensor_type]
    module = __import__(module_name, fromlist=["main"])
    module.main()


if __name__ == "__main__":
    main()
