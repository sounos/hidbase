# Authority: SounOS.org
Summary: High performence distributed Database 
Name: hidbase
Version: 0.0.3
Release: 51%{?dist}
License: BSD
Group: System Environment/Libraries
URL: http://code.google.com/p/hidbase/

Source: http://code.google.com/p/hidbase/download/%{name}-%{version}.tar.gz
Packager: SounOS <SounOS@gmail.com>
Vendor: SounOS
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildRequires: libevbase >= 1.0.0 libsbase >= 1.0.0
Requires: libevbase >= 1.0.0 libsbase >= 1.0.0

%description
High performence distributed Database

%package -n libdbase
Group: Development/Libraries
Summary: Development tools for the hidbase server.

%description -n libdbase
The package "libdbase" contains the API files libdbase.so* and dbase.h

%prep
%setup

%build
%configure
%{__make}

%install
%{__rm} -rf %{buildroot}
%{__make} install DESTDIR="%{buildroot}"

%clean
%{__rm} -rf %{buildroot}

%post 

    /sbin/chkconfig --add hitrackerd 
    /sbin/chkconfig --add hichunkd 

%preun 
    [ "`pstree|grep hitrackerd|wc -l`" -gt "0" ] && /sbin/service hitrackerd stop
    [ "`pstree|grep hichunkd|wc -l`" -gt "0" ] && /sbin/service hichunkd stop
    /sbin/chkconfig --del hitrackerd
    /sbin/chkconfig --del hichunkd

%files -n libdbase
%defattr(-,root,root)
%{_includedir}/*
%{_libdir}/*

%files
%defattr(-, root, root, 0755)
%{_sbindir}/*
%{_localstatedir}/*
%{_sysconfdir}/rc.d/*
%config(noreplace) %{_sysconfdir}/*.ini

%changelog
* Tue Jun 21 2011 23:05:05 SounOS <sounos@gmail.com>
- updated dbase.c
- updated xmm.c
- updated dbase.*
