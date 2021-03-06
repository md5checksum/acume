Name:        reflex-acume-solution
Version:     @@RPM_VERSION@@
Release:     @@RPM_RELEASE@@%{?dist}
Summary:     The Reflex Third Party Software Manager.
Vendor:      Guavus Network Systems
License:     Proprietary
URL:         http://www.guavus.com
BuildRoot:   %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
Packager:    Reflex ACUME Solution (acume@guavus.com)
Source0:     reflex-acume-solution-%{version}.tar
Requires:    reflex-acume-base >= %{version}, reflex-acume-base < @@RPM_VERSION_CHANGE@@
Requires:    reflex-dataminingUdaf >= %{version}, reflex-dataminingUdaf < @@RPM_VERSION_CHANGE@@
Requires:    reflex-crux >= %{version}, reflex-crux < @@RPM_VERSION_CHANGE@@

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
