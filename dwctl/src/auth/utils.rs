
pub fn default_display_name(email: &str) -> String {
    let prefix = email.rsplit_once('@').map_or(email, |(local, _)| local);
    if prefix.is_empty() {
        "user".to_string()
    } else {
        prefix.to_string()
    }
}

pub fn email_domain(email: &str) -> Option<String> {
    let (_, domain) = email.rsplit_once('@')?;
    let domain = domain.trim().trim_end_matches('.').to_ascii_lowercase();
    is_claimable_domain_shape(&domain).then_some(domain)
}

pub fn is_claimable_domain_shape(domain: &str) -> bool {
    let mut labels = domain.split('.').peekable();
    let mut count = 0;
    while let Some(label) = labels.next() {
        count += 1;
        let valid = (1..=63).contains(&label.len())
            && label.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && !label.starts_with('-')
            && !label.ends_with('-');
        if !valid {
            return false;
        }
        if labels.peek().is_none() && count < 2 {
            return false;
        }
    }
    count >= 2
}

pub fn is_builtin_personal_email_domain(domain: &str) -> bool {
    const PERSONAL_DOMAINS: &[&str] = &[
        "gmail.com",
        "googlemail.com",
        "hotmail.com",
        "hotmail.co.uk",
        "hotmail.fr",
        "hotmail.de",
        "hotmail.es",
        "hotmail.it",
        "hotmail.be",
        "hotmail.nl",
        "hotmail.se",
        "hotmail.ca",
        "hotmail.com.au",
        "hotmail.com.br",
        "live.com",
        "live.fr",
        "live.co.uk",
        "live.de",
        "live.it",
        "live.nl",
        "live.se",
        "live.ca",
        "live.com.au",
        "live.com.mx",
        "outlook.com",
        "outlook.fr",
        "outlook.de",
        "outlook.es",
        "outlook.it",
        "outlook.be",
        "outlook.dk",
        "outlook.ie",
        "outlook.jp",
        "outlook.co.id",
        "outlook.com.au",
        "outlook.com.br",
        "msn.com",
        "msn.co.uk",
        "passport.com",
        "windowslive.com",
        "yahoo.com",
        "yahoo.co.uk",
        "yahoo.co.jp",
        "yahoo.co.in",
        "yahoo.co.id",
        "yahoo.co.kr",
        "yahoo.co.nz",
        "yahoo.com.ar",
        "yahoo.com.au",
        "yahoo.com.br",
        "yahoo.com.hk",
        "yahoo.com.mx",
        "yahoo.com.ph",
        "yahoo.com.sg",
        "yahoo.com.tw",
        "yahoo.com.vn",
        "yahoo.de",
        "yahoo.dk",
        "yahoo.es",
        "yahoo.fr",
        "yahoo.gr",
        "yahoo.ie",
        "yahoo.in",
        "yahoo.it",
        "yahoo.nl",
        "yahoo.no",
        "yahoo.pl",
        "yahoo.pt",
        "yahoo.ro",
        "yahoo.se",
        "ymail.com",
        "rocketmail.com",
        "aol.com",
        "aol.co.uk",
        "aol.de",
        "aol.fr",
        "aim.com",
        "icloud.com",
        "me.com",
        "mac.com",
        "mail.com",
        "email.com",
        "usa.com",
        "zoho.com",
        "zohomail.com",
        "yandex.com",
        "yandex.ru",
        "yandex.by",
        "yandex.kz",
        "yandex.ua",
        "ya.ru",
        "mail.ru",
        "bk.ru",
        "inbox.ru",
        "list.ru",
        "internet.ru",
        "rambler.ru",
        "ukr.net",
        "meta.ua",
        "qq.com",
        "foxmail.com",
        "163.com",
        "126.com",
        "yeah.net",
        "sina.com",
        "sina.cn",
        "sohu.com",
        "aliyun.com",
        "tom.com",
        "21cn.com",
        "naver.com",
        "hanmail.net",
        "daum.net",
        "nate.com",
        "docomo.ne.jp",
        "ezweb.ne.jp",
        "softbank.ne.jp",
        "nifty.com",
        "biglobe.ne.jp",
        "rediffmail.com",
        "sify.com",
        "protonmail.com",
        "protonmail.ch",
        "proton.me",
        "pm.me",
        "tutanota.com",
        "tutanota.de",
        "tutamail.com",
        "tuta.com",
        "tuta.io",
        "fastmail.com",
        "fastmail.fm",
        "hushmail.com",
        "posteo.de",
        "mailbox.org",
        "runbox.com",
        "startmail.com",
        "disroot.org",
        "riseup.net",
        "zoho.eu",
        "gmx.com",
        "gmx.de",
        "gmx.net",
        "gmx.at",
        "gmx.ch",
        "gmx.fr",
        "gmx.co.uk",
        "gmx.es",
        "web.de",
        "t-online.de",
        "freenet.de",
        "arcor.de",
        "online.de",
        "email.de",
        "libero.it",
        "virgilio.it",
        "alice.it",
        "tiscali.it",
        "tin.it",
        "fastwebnet.it",
        "wanadoo.fr",
        "orange.fr",
        "free.fr",
        "sfr.fr",
        "laposte.net",
        "neuf.fr",
        "bbox.fr",
        "club-internet.fr",
        "voila.fr",
        "terra.es",
        "telefonica.net",
        "wp.pl",
        "o2.pl",
        "onet.pl",
        "interia.pl",
        "gazeta.pl",
        "seznam.cz",
        "centrum.cz",
        "post.cz",
        "abv.bg",
        "mail.bg",
        "freemail.hu",
        "citromail.hu",
        "sapo.pt",
        "telenet.be",
        "skynet.be",
        "ziggo.nl",
        "kpnmail.nl",
        "home.nl",
        "telia.com",
        "bredband.net",
        "online.no",
        "bluewin.ch",
        "hispeed.ch",
        "mynet.com",
        "hotmail.com.tr",
        "comcast.net",
        "verizon.net",
        "att.net",
        "sbcglobal.net",
        "bellsouth.net",
        "cox.net",
        "charter.net",
        "earthlink.net",
        "juno.com",
        "netzero.net",
        "optonline.net",
        "roadrunner.com",
        "rr.com",
        "windstream.net",
        "frontier.com",
        "btinternet.com",
        "sky.com",
        "virginmedia.com",
        "talktalk.net",
        "ntlworld.com",
        "blueyonder.co.uk",
        "tiscali.co.uk",
        "plus.net",
        "bigpond.com",
        "bigpond.net.au",
        "optusnet.com.au",
        "iinet.net.au",
        "tpg.com.au",
        "xtra.co.nz",
        "shaw.ca",
        "rogers.com",
        "sympatico.ca",
        "telus.net",
        "videotron.ca",
        "uol.com.br",
        "bol.com.br",
        "terra.com.br",
        "ig.com.br",
        "globo.com",
        "prodigy.net.mx",
        "privaterelay.appleid.com",
        "mozmail.com",
        "duck.com",
        "passmail.net",
        "simplelogin.com",
        "anonaddy.com",
        "addy.io",
        "relay.firefox.com",
        "mailinator.com",
        "guerrillamail.com",
        "sharklasers.com",
        "10minutemail.com",
        "yopmail.com",
        "trashmail.com",
        "getnada.com",
        "temp-mail.org",
        "tempmail.com",
        "throwawaymail.com",
        "maildrop.cc",
        "dispostable.com",
        "fakeinbox.com",
        "mailnesia.com",
        "spamgourmet.com",
        "moakt.com",
        "emailondeck.com",
    ];

    let lower = domain.to_lowercase();
    PERSONAL_DOMAINS.contains(&lower.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_claimed_providers_are_personal() {
        for domain in ["qq.com", "yandex.ru", "comcast.net", "web.de", "126.com"] {
            assert!(is_builtin_personal_email_domain(domain), "{domain} must not be claimable");
        }
    }

    #[test]
    fn regional_variants_are_personal() {
        for domain in ["yahoo.fr", "yahoo.co.in", "hotmail.de", "live.co.uk", "outlook.fr", "gmx.at"] {
            assert!(is_builtin_personal_email_domain(domain), "{domain} must not be claimable");
        }
    }

    #[test]
    fn company_domains_stay_claimable() {
        for domain in ["doubleword.ai", "acme.com", "mycomcast.net", "qq.com.mycompany.io"] {
            assert!(!is_builtin_personal_email_domain(domain), "{domain} must stay claimable");
        }
    }

    #[test]
    fn personal_check_is_case_insensitive() {
        assert!(is_builtin_personal_email_domain("GMAIL.com"));
        assert!(is_builtin_personal_email_domain("QQ.COM"));
    }

    #[test]
    fn email_domain_rejects_shapes_that_are_not_dns_names() {
        assert_eq!(email_domain("person@%"), None);
        assert_eq!(email_domain("person@_"), None);
        assert_eq!(email_domain("person@%.com"), None);
        assert_eq!(email_domain("person@acme_.com"), None);
        assert_eq!(email_domain("person@user"), None);
        assert_eq!(email_domain("person@localhost"), None);
        assert_eq!(email_domain("person@acme..com"), None);
        assert_eq!(email_domain("person@-acme.com"), None);
        assert_eq!(email_domain("person@acme-.com"), None);
        assert_eq!(email_domain("person@ac me.com"), None, "an inner space is not a valid label");
        assert_eq!(
            email_domain("person@ acme.com "),
            Some("acme.com".to_string()),
            "but surrounding whitespace is just trimmed"
        );
        assert_eq!(email_domain("person@ácme.com"), None);
        assert_eq!(email_domain("person@acme.com"), Some("acme.com".to_string()));
        assert_eq!(email_domain("person@mail.acme.co.uk"), Some("mail.acme.co.uk".to_string()));
        assert_eq!(email_domain("person@xn--cme-9na.com"), Some("xn--cme-9na.com".to_string()));
        assert_eq!(email_domain("person@a-1.acme-corp.io"), Some("a-1.acme-corp.io".to_string()));
    }

    #[test]
    fn email_domain_normalises_and_rejects_empties() {
        assert_eq!(email_domain("bob@Acme.COM"), Some("acme.com".to_string()));
        assert_eq!(email_domain("bob@acme.com."), Some("acme.com".to_string()));
        assert_eq!(email_domain("bob@"), None);
        assert_eq!(email_domain("bob@   "), None);
        assert_eq!(email_domain("no-at-sign"), None);
    }
}
