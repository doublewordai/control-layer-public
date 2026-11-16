use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

pub fn is_denied(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_denied_v4(v4),
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_denied_v4(mapped);
            }
            is_denied_v6(v6)
        }
    }
}

fn is_denied_v4(ip: Ipv4Addr) -> bool {
    if ip.is_loopback() || ip.is_link_local() || ip.is_broadcast() || ip.is_multicast() || ip.is_unspecified() || ip.is_documentation() {
        return true;
    }
    if ip.is_private() {
        return true;
    }
    let octets = ip.octets();
    if octets[0] == 100 && (octets[1] & 0b1100_0000) == 0b0100_0000 {
        return true;
    }
    if octets[0] == 0 {
        return true;
    }
    if octets[0] >= 240 {
        return true;
    }
    false
}

fn is_denied_v6(ip: Ipv6Addr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() {
        return true;
    }
    let segments = ip.segments();
    if (segments[0] & 0xffc0) == 0xfe80 {
        return true;
    }
    if (segments[0] & 0xfe00) == 0xfc00 {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v4(s: &str) -> IpAddr {
        IpAddr::V4(s.parse().unwrap())
    }
    fn v6(s: &str) -> IpAddr {
        IpAddr::V6(s.parse().unwrap())
    }

    #[test]
    fn allows_typical_public_v4() {
        assert!(!is_denied(v4("8.8.8.8")));
        assert!(!is_denied(v4("1.1.1.1")));
        assert!(!is_denied(v4("142.250.190.78")));
    }

    #[test]
    fn denies_loopback_v4() {
        assert!(is_denied(v4("127.0.0.1")));
        assert!(is_denied(v4("127.1.2.3")));
    }

    #[test]
    fn denies_rfc1918() {
        assert!(is_denied(v4("10.0.0.1")));
        assert!(is_denied(v4("10.255.255.255")));
        assert!(is_denied(v4("172.16.0.1")));
        assert!(is_denied(v4("172.31.255.255")));
        assert!(is_denied(v4("192.168.1.1")));
    }

    #[test]
    fn rfc1918_172_boundaries_correct() {
        assert!(!is_denied(v4("172.15.0.1")));
        assert!(!is_denied(v4("172.32.0.1")));
    }

    #[test]
    fn denies_link_local_including_metadata() {
        assert!(is_denied(v4("169.254.0.1")));
        assert!(is_denied(v4("169.254.169.254")));
        assert!(is_denied(v4("169.254.255.255")));
    }

    #[test]
    fn denies_cgnat() {
        assert!(is_denied(v4("100.64.0.1")));
        assert!(is_denied(v4("100.127.255.255")));
        assert!(!is_denied(v4("100.63.255.255")));
        assert!(!is_denied(v4("100.128.0.0")));
    }

    #[test]
    fn denies_broadcast_multicast_unspecified() {
        assert!(is_denied(v4("0.0.0.0")));
        assert!(is_denied(v4("255.255.255.255")));
        assert!(is_denied(v4("224.0.0.1")));
    }

    #[test]
    fn denies_reserved_future() {
        assert!(is_denied(v4("240.0.0.1")));
        assert!(is_denied(v4("250.10.20.30")));
    }

    #[test]
    fn denies_v6_loopback() {
        assert!(is_denied(v6("::1")));
    }

    #[test]
    fn denies_v6_unspecified() {
        assert!(is_denied(v6("::")));
    }

    #[test]
    fn denies_v6_link_local() {
        assert!(is_denied(v6("fe80::1")));
        assert!(is_denied(v6("fe80::1234:5678:9abc:def0")));
    }

    #[test]
    fn denies_v6_unique_local() {
        assert!(is_denied(v6("fc00::1")));
        assert!(is_denied(v6("fd12:3456::1")));
    }

    #[test]
    fn denies_v6_multicast() {
        assert!(is_denied(v6("ff02::1")));
    }

    #[test]
    fn allows_typical_public_v6() {
        assert!(!is_denied(v6("2001:4860:4860::8888")));
        assert!(!is_denied(v6("2606:4700:4700::1111")));
    }

    #[test]
    fn unmaps_v4_mapped_v6_to_apply_v4_rules() {
        assert!(is_denied(v6("::ffff:10.0.0.1")));
        assert!(is_denied(v6("::ffff:169.254.169.254")));
        assert!(!is_denied(v6("::ffff:8.8.8.8")));
    }
}
