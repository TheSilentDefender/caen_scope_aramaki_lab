from pathlib import Path
from typing import Dict, Any, Union
import configparser
from dataclasses import dataclass

@dataclass
class ChannelSettings:
    """Dataclass to store channel-specific settings"""
    dc_offset: int
    threshold: int

@dataclass
class AcquisitionSettings:
    """Dataclass to store acquisition settings"""
    record_length: int
    pretrigger: int
    acq_trigger_source: str
    trigger_mode: str
    selected_channels: str
    trigger_mask: str

class ConfigurationError(Exception):
    """Custom exception for configuration-related errors"""
    pass

class ConfigReader:
    """
    Configuration reader using ConfigParser with default fallbacks
    """
    def __init__(
        self,
        file_path: Union[str, Path],
        default_settings: Dict[str, Dict[str, Any]],
        num_channels: int
    ):
        self.file_path = Path(file_path)
        self.default_settings = default_settings
        self.num_channels = num_channels
        self.config = configparser.ConfigParser(
            interpolation=None,
            comment_prefixes=('#', ';'),
            empty_lines_in_values=False,
            strict=True
        )
        
        # Initialize with empty config and load existing settings if any
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from INI file if it exists"""
        try:
            if self.file_path.exists():
                with self.file_path.open('r') as f:
                    self.config.read_file(f)
        except configparser.Error as e:
            raise ConfigurationError(f"Error reading configuration file: {str(e)}")
    
    def _save_current_config(self) -> None:
        """Save current configuration to INI file"""
        try:
            with self.file_path.open('w') as f:
                self.config.write(f)
        except Exception as e:
            raise ConfigurationError(f"Error saving configuration: {str(e)}")
    
    def _get_with_default(self, section: str, option: str) -> str:
        """
        Get configuration value with fallback to defaults
        
        Parameters:
        - section: Configuration section name
        - option: Option name within the section
        """
        try:
            # For channel settings
            if section.startswith("CH"):
                if self.config.has_option(section, option):
                    return self.config.get(section, option)
                return self.default_settings["default_channel"][option]
            
            # For ACQ settings
            if self.config.has_option(section, option):
                return self.config.get(section, option)
            return self.default_settings[section][option]
            
        except (KeyError, configparser.Error) as e:
            raise ConfigurationError(f"Error getting configuration value: {str(e)}")
    
    def _get_int_with_default(self, section: str, option: str) -> int:
        """Get integer configuration value with fallback to defaults"""
        value = self._get_with_default(section, option)
        try:
            return int(value)
        except ValueError as e:
            raise ConfigurationError(f"Error converting value to integer: {str(e)}")
    
    def get_acquisition_settings(self) -> AcquisitionSettings:
        """Get typed acquisition settings"""
        try:
            return AcquisitionSettings(
                record_length=self._get_int_with_default("ACQ", "record_length"),
                pretrigger=self._get_int_with_default("ACQ", "pretrigger"),
                acq_trigger_source=self._get_with_default("ACQ", "acq_trigger_source"),
                trigger_mode=self._get_with_default("ACQ", "trigger_mode"),
                selected_channels=self._get_with_default("ACQ", "selected_channels"),
                trigger_mask=self._get_with_default("ACQ", "trigger_mask")
            )
        except (ConfigurationError, KeyError, ValueError) as e:
            raise ConfigurationError(f"Error getting acquisition settings: {str(e)}")
    
    def get_channel_settings(self, channel: int) -> ChannelSettings:
        """Get typed channel settings"""
        if not 0 <= channel < self.num_channels:
            raise ValueError(f"Invalid channel number: {channel}")
        
        try:
            section = f"CH{channel}"
            return ChannelSettings(
                dc_offset=self._get_int_with_default(section, "dc_offset"),
                threshold=self._get_int_with_default(section, "threshold")
            )
        except (ConfigurationError, KeyError, ValueError) as e:
            raise ConfigurationError(f"Error getting channel settings: {str(e)}")
    
    def get(self, section: str, option: str) -> str:
        """Get raw configuration value with default fallback"""
        return self._get_with_default(section, option)
    
    def set(self, section: str, option: str, value: Any) -> None:
        """
        Set configuration value and save
        
        Parameters:
        - section: Configuration section name
        - option: Option name within the section
        - value: Value to set
        """
        try:
            if section not in self.config:
                self.config.add_section(section)
            self.config.set(section, option, str(value))
            self._save_current_config()
        except configparser.Error as e:
            raise ConfigurationError(f"Error setting configuration value: {str(e)}")
    
    def remove_option(self, section: str, option: str) -> bool:
        """
        Remove an option from the configuration
        
        Returns True if the option was removed, False if it didn't exist
        """
        try:
            result = self.config.remove_option(section, option)
            if result:
                self._save_current_config()
            return result
        except configparser.Error as e:
            raise ConfigurationError(f"Error removing option: {str(e)}")
    
    def remove_section(self, section: str) -> bool:
        """
        Remove an entire section from the configuration
        
        Returns True if the section was removed, False if it didn't exist
        """
        try:
            result = self.config.remove_section(section)
            if result:
                self._save_current_config()
            return result
        except configparser.Error as e:
            raise ConfigurationError(f"Error removing section: {str(e)}")
    
    def display_config(self) -> None:
        """Display current configuration with defaults indicated"""
        # Display explicitly set values
        print("Current Configuration (explicitly set values):")
        for section in self.config.sections():
            print(f"[{section}]")
            for key, value in self.config[section].items():
                print(f"{key} = {value}")
            print()
        
        # Display defaults
        print("\nDefault Values:")
        print("[ACQ] (defaults)")
        for key, value in self.default_settings["ACQ"].items():
            if not (self.config.has_section("ACQ") and 
                   self.config.has_option("ACQ", key)):
                print(f"{key} = {value} (default)")
        
        print("\n[Channel Defaults]")
        for key, value in self.default_settings["default_channel"].items():
            print(f"{key} = {value}")

# Example usage:
if __name__ == "__main__":
    default_settings = {
        "ACQ": {
            "record_length": "300000",
            "pretrigger": "16336",
            "acq_trigger_source": "SwTrg",
            "trigger_mode": "Normal",
            "selected_channels": "0x7",
            "trigger_mask": "0x0",
        },
        "default_channel": {
            "dc_offset": "50",
            "threshold": "100",
        },
    }

    # Create config reader instance
    config_reader = ConfigReader("settings.ini", default_settings, 3)
    
    # Example operations
    try:
        # Get settings - will use defaults if not explicitly set
        acq_settings = config_reader.get_acquisition_settings()
        print(f"Record length: {acq_settings.record_length}")
        
        # Get channel settings - will use defaults if not explicitly set
        ch0_settings = config_reader.get_channel_settings(0)
        print(f"Channel 0 threshold: {ch0_settings.threshold}")
        
        # Set a specific value (this will be saved to file)
        config_reader.set("CH0", "dc_offset", "75")
        
        # Display configuration showing both explicit and default values
        print("\nConfiguration with defaults:")
        config_reader.display_config()
        
    except ConfigurationError as e:
        print(f"Configuration error: {e}")