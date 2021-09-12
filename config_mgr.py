from yaml import load, FullLoader

"""
Contains ConfigMgr and ConfiguratinError classes
ConfigMgr loads a YAML configuration file and 
loads application settings for the specified 
environment.
"""


class ConfigurationError(Exception):
	"""
	Custom configuration exception
	"""
	pass


class ConfigMgr:
	"""
	Repository of settings for application
	"""

	def __init__(self, config_file_path, environment="default"):
		"""
		Initialize ConfigMgr and load environment-specific settings
		:param config_file_path: Path to yaml configuration file
		:param environment: environment to choose in yaml file
		"""
		with open(config_file_path, "r") as file:
			configuration = load(file, Loader=FullLoader)
			if environment not in configuration:
				raise ConfigurationError(f"Configuration error: {environment} not found in {config_file_path}")
			self.settings = configuration[environment]

	def get(self, setting):
		if setting in self.settings:
			return self.settings[setting]
		else:
			raise ConfigurationError(f"Setting {setting} not found in configuration")
