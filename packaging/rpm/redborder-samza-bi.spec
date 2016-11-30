Name: redborder-samza-bi
Version: %{__version}
Release: %{__release}%{?dist}
BuildArch: noarch
Summary: redborder samza bi package with enrichment and indexing samza applications

License: AGPL 3.0
URL: https://github.com/redBorder/rb-samza-bi
Source0: rb-samza-bi-%{__tagversion}-SNAPSHOT-dist.tar.gz

Requires: java 

%description
%{summary}

#%prep
#%setup -qn rb-samza-bi-%{version}

#%build

%install
mkdir -p %{buildroot}/usr/lib/samza/rb-samza-bi/app
install -D -m 644 %{SOURCE0} %{buildroot}/usr/lib/samza/rb-samza-bi/app

%clean
rm -rf %{buildroot}

%files
%defattr(0644,root,root)
/usr/lib/samza/rb-samza-bi/app/rb-samza-bi-%{__tagversion}-SNAPSHOT-dist.tar.gz

%changelog
* Wed Nov 30 2016 Juan J. Prieto <jjprieto@redborder.com> - 0.0.2-1
- Packaging with make, maven and sbt

* Tue Nov 29 2016 Alberto Rodriguez <arodriguez@redborder.com> - 0.0.1-1
- first spec version
