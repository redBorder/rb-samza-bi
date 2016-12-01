%global __samzabihome /usr/lib/samza/rb-samza-bi

Name: redborder-samza-bi
Version: %{__version}
Release: %{__release}%{?dist}
BuildArch: noarch
Summary: redborder samza bi package with enrichment and indexing samza applications

License: AGPL 3.0
URL: https://github.com/redBorder/rb-samza-bi
Source0: %{name}-%{version}.tar.gz
Source1: rb-samza-bi-%{__mvnversion}-SNAPSHOT-dist.tar.gz

Requires: java redborder-samza

%description
%{summary}

%install
mkdir -p %{buildroot}%{__samzabihome}/app
install -D -m 644 %{SOURCE1} %{buildroot}%{__samzabihome}/app
pushd %{buildroot}%{__samzabihome}/app &>/dev/null
ln -s rb-samza-bi-%{__mvnversion}-SNAPSHOT-dist.tar.gz rb-samza-bi-%{version}.tar.gz
ln -s rb-samza-bi-%{version}.tar.gz rb-samza-bi.tar.gz
popd &>/dev/null

%clean
rm -rf %{buildroot}

%files
%defattr(0644,root,root)
%{__samzabihome}/app/rb-samza-bi-%{__mvnversion}-SNAPSHOT-dist.tar.gz
%{__samzabihome}/app/rb-samza-bi-%{version}.tar.gz
%{__samzabihome}/app/rb-samza-bi.tar.gz

%changelog
* Wed Nov 30 2016 Juan J. Prieto <jjprieto@redborder.com> - 0.0.2-1
- Packaging with make, maven and sbt

* Tue Nov 29 2016 Alberto Rodriguez <arodriguez@redborder.com> - 0.0.1-1
- first spec version
