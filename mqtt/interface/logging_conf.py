# coding=utf-8
import logging


def configure_mqtt_interface_logging(level=logging.INFO):
    """Configure root logger with millisecond-precision timestamps for mqtt interface.

    Only configure if no handlers are present to avoid clobbering an existing setup.
    """
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=level,
            format='%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )


# Configure on import for convenience
configure_mqtt_interface_logging()
