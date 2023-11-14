use std::net::Ipv4Addr;
use std::str::FromStr;
use crate::error::SddmsError;

/// Splits a command line host string into ip address and port
pub fn split_host_string(host_str: &str) -> Result<(Ipv4Addr, u16), SddmsError> {
    let parts: Vec<&str> = host_str.split(":")
        .collect();

    if parts.len() != 2 {
        return Err(SddmsError::general(format!("Host string '{}' is ill-formed. It has {} parts", host_str, parts.len())));
    }

    let host_str = parts[0];
    let ip_addr = Ipv4Addr::from_str(host_str)
        .map_err(|err| SddmsError::general("Could not parse ip addr").with_cause(err))?;
    let port_str = parts[1];
    let port: u16 = port_str.parse()
        .map_err(|err| SddmsError::general("Could not parse port").with_cause(err))?;

    Ok((ip_addr, port))
}

#[cfg(test)]
mod tests {
    use crate::host_utils::split_host_string;

    #[test]
    fn split_host_string__succeeds() {
        let input = "0.0.0.0:50051";
        let result = split_host_string(input);
        assert!(result.is_ok());
        let (ip_addr, port) = result.unwrap();
        assert!(ip_addr.is_unspecified());
        assert_eq!(port, 50051);
    }
    
    #[test]
    fn split_host_string__fails__when_invalid_ip() {
        let input = "0.0.12.345:500";
        let result = split_host_string(input);
        assert!(result.is_err());
    }
}
