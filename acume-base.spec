Name:        reflex-acume-base
Version:     @@VERSION@@
Release:     1%{?dist}
Summary:     The Reflex Third Party Software Manager.
Vendor:      Guavus Network Systems
License:     Proprietary
URL:         http://www.guavus.com
BuildRoot:   %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
Packager:    Reflex ACUME Base (acume@guavus.com)
Source0:     reflex-acume-base-@@VERSION@@.tar
Requires:    reflex-hadoop = %{version}-%{release}, reflex-sparksql = %{version}-%{release}, reflex-base = %{version}-%{release},reflex-insta-as-api = %{version}-%{release}

#SOURCE1:    filter_perl_requires.sh

%define debug_package %{nil}

%define _unpackaged_files_terminate_build 0

%global __os_install_post %{nil}

%global reflex_root_prefix /opt/reflex
%global reflex_user reflex

#%define __perl_requires %{SOURCE1}

%description
The Reflex Third Party Software Manager.

%prep
%setup -q

%build
# We don't build. We just install.

%install

#
# We cannot do this in the RPM creating script because of assumptions of
# rpmbuild wrt to the Source section.
#
mkdir -p ${RPM_BUILD_ROOT}/%{reflex_root_prefix}
cp -rfP . ${RPM_BUILD_ROOT}/%{reflex_root_prefix}

%clean
rm -rf %{buildroot}

%pre

%post
ldconfig

    if [[ "$1" = "1" ]]; then

        chown -R %{reflex_user}:%{reflex_user} \
            %{reflex_root_prefix}/vtmp \
            %{reflex_root_prefix}/config \
            %{reflex_root_prefix}/var \
            %{reflex_root_prefix}/data

    fi

%preun

%postun

%files
#%attr(0644, root, root) /etc/sudoers.d/sudoers_reflex
#%attr(-, reflex, reflex) %config /reflex/config/db
#%attr(-, reflex, reflex) /reflex
#/*
%attr(-, root, root) /


%changelog
