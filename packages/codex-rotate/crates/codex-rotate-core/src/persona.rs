use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OsFamily {
    Macos,
    Windows,
    Linux,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PersonaProfileContract {
    pub persona_profile_id: String,
    pub os_family: OsFamily,
    pub locale: String,
    pub language: String,
    pub timezone: String,
    pub hostname_style: String,
    pub browser: BrowserProfileDefaults,
    pub vm_hardware: VmHardwareProfileDefaults,
    pub device_scale_factor: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrowserProfileDefaults {
    pub user_agent_family: String,
    pub vendor: String,
    pub renderer: String,
    pub user_agent: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VmHardwareProfileDefaults {
    pub cpu_cores: u32,
    pub memory_mb: u32,
    pub display_resolution: String,
    pub display_scaling: String,
    pub screen_width: u32,
    pub screen_height: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HostPersonaDefaults {
    pub os_family: OsFamily,
    pub locale: String,
    pub language: String,
    pub timezone: String,
    pub hostname_style: String,
    pub browser: BrowserProfileDefaults,
    pub screen_width: u32,
    pub screen_height: u32,
    pub device_scale_factor: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VmPersonaDefaults {
    pub os_family: OsFamily,
    pub locale: String,
    pub language: String,
    pub timezone: String,
    pub hostname_style: String,
    pub browser: BrowserProfileDefaults,
    pub vm_hardware: VmHardwareProfileDefaults,
}

impl PersonaProfileContract {
    pub fn host_defaults(&self) -> HostPersonaDefaults {
        HostPersonaDefaults {
            os_family: self.os_family.clone(),
            locale: self.locale.clone(),
            language: self.language.clone(),
            timezone: self.timezone.clone(),
            hostname_style: self.hostname_style.clone(),
            browser: self.browser.clone(),
            screen_width: self.vm_hardware.screen_width,
            screen_height: self.vm_hardware.screen_height,
            device_scale_factor: self.device_scale_factor,
        }
    }

    pub fn vm_defaults(&self) -> VmPersonaDefaults {
        VmPersonaDefaults {
            os_family: self.os_family.clone(),
            locale: self.locale.clone(),
            language: self.language.clone(),
            timezone: self.timezone.clone(),
            hostname_style: self.hostname_style.clone(),
            browser: self.browser.clone(),
            vm_hardware: self.vm_hardware.clone(),
        }
    }
}

/// Returns the authoritative mapping from persona_profile_id to realism defaults.
pub fn get_persona_profile(id: &str) -> PersonaProfileContract {
    match id {
        "balanced-us-compact" => PersonaProfileContract {
            persona_profile_id: id.to_string(),
            os_family: OsFamily::Macos,
            locale: "en_US".to_string(),
            language: "en-US".to_string(),
            timezone: "America/New_York".to_string(),
            hostname_style: "macbook-pro".to_string(),
            browser: BrowserProfileDefaults {
                user_agent_family: "chrome".to_string(),
                vendor: "Google Inc.".to_string(),
                renderer: "WebKit".to_string(),
                user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36".to_string(),
            },
            vm_hardware: VmHardwareProfileDefaults {
                cpu_cores: 4,
                memory_mb: 8192,
                display_resolution: "2560x1600".to_string(),
                display_scaling: "2.0".to_string(),
                screen_width: 1280,
                screen_height: 800,
            },
            device_scale_factor: 2.0,
        },
        "balanced-eu-wide" => PersonaProfileContract {
            persona_profile_id: id.to_string(),
            os_family: OsFamily::Macos,
            locale: "en_GB".to_string(),
            language: "en-GB".to_string(),
            timezone: "Europe/London".to_string(),
            hostname_style: "imac".to_string(),
            browser: BrowserProfileDefaults {
                user_agent_family: "chrome".to_string(),
                vendor: "Google Inc.".to_string(),
                renderer: "WebKit".to_string(),
                user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36".to_string(),
            },
            vm_hardware: VmHardwareProfileDefaults {
                cpu_cores: 6,
                memory_mb: 16384,
                display_resolution: "2880x1800".to_string(),
                display_scaling: "2.0".to_string(),
                screen_width: 1440,
                screen_height: 900,
            },
            device_scale_factor: 2.0,
        },
        "balanced-apac-standard" => PersonaProfileContract {
            persona_profile_id: id.to_string(),
            os_family: OsFamily::Macos,
            locale: "en_SG".to_string(),
            language: "en-SG".to_string(),
            timezone: "Asia/Singapore".to_string(),
            hostname_style: "mac-studio".to_string(),
            browser: BrowserProfileDefaults {
                user_agent_family: "chrome".to_string(),
                vendor: "Google Inc.".to_string(),
                renderer: "WebKit".to_string(),
                user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36".to_string(),
            },
            vm_hardware: VmHardwareProfileDefaults {
                cpu_cores: 8,
                memory_mb: 32768,
                display_resolution: "3024x1964".to_string(),
                display_scaling: "2.0".to_string(),
                screen_width: 1512,
                screen_height: 982,
            },
            device_scale_factor: 2.0,
        },
        _ => get_persona_profile("balanced-us-compact"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_persona_profile() {
        let us = get_persona_profile("balanced-us-compact");
        assert_eq!(us.locale, "en_US");
        assert_eq!(us.vm_hardware.cpu_cores, 4);

        let eu = get_persona_profile("balanced-eu-wide");
        assert_eq!(eu.locale, "en_GB");
        assert_eq!(eu.vm_hardware.cpu_cores, 6);

        let apac = get_persona_profile("balanced-apac-standard");
        assert_eq!(apac.locale, "en_SG");
        assert_eq!(apac.vm_hardware.cpu_cores, 8);

        let default = get_persona_profile("unknown");
        assert_eq!(default.persona_profile_id, "balanced-us-compact");
    }

    #[test]
    fn test_host_defaults_exclusion() {
        let profile = get_persona_profile("balanced-us-compact");
        let host = profile.host_defaults();
        assert_eq!(host.locale, "en_US");
        assert_eq!(host.screen_width, 1280);
        assert_eq!(host.device_scale_factor, 2.0);
    }

    #[test]
    fn test_vm_defaults_inclusion() {
        let profile = get_persona_profile("balanced-eu-wide");
        let vm = profile.vm_defaults();
        assert_eq!(vm.locale, "en_GB");
        assert_eq!(vm.vm_hardware.cpu_cores, 6);
        assert_eq!(vm.vm_hardware.memory_mb, 16384);
        assert_eq!(vm.vm_hardware.display_resolution, "2880x1800");
    }
}
