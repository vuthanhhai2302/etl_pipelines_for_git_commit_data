import yaml

class ConfigLoader:
    def __init__(self, config_file: str):
        self.config_file = config_file
        self.config_data = self._load_config()

    def _load_config(self):
        """
        Private method to load the YAML configuration file.

        Returns:
            dict: The configuration data loaded from the YAML file.

        Raises:
            Exception: If an error occurs while loading the file.
        """
        try:
            with open(self.config_file, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            print(f"Error loading configuration: {e}")
            raise

    def get(self, section: str, key: str, default=None):
        """Get a value from the configuration."""
        try:
            return self.config_data.get(section, {}).get(key, default)
        except KeyError:
            print(f"Key {key} not found in section {section}")
            return default
        