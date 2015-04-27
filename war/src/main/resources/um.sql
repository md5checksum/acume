#VER 3.5.1
create table GroupInfo (groupName varchar(255) not null, version integer not null, primary key (groupName));
create table Publication (reportId integer not null, version integer not null, primary key (reportId));
create table UserInfo (userName varchar(255) not null, email varchar(255), enabled boolean not null, password varchar(255), version integer not null, primary key (userName));
create table publication_group (reportId integer not null, groupName varchar(255) not null, primary key (reportId, groupName));
create table publication_user (reportId integer not null, userName varchar(255) not null, primary key (reportId, userName));
create table user_group (userName varchar(255) not null, groupName varchar(255) not null, primary key (userName, groupName));
alter table publication_group add constraint FK3889AAAC2CA3DDF9 foreign key (reportId) references Publication;
alter table publication_group add constraint FK3889AAACEB8E9D07 foreign key (groupName) references GroupInfo;
alter table publication_user add constraint FK9EC07DFE44F54FE3 foreign key (userName) references UserInfo;
alter table publication_user add constraint FK9EC07DFE2CA3DDF9 foreign key (reportId) references Publication;
alter table user_group add constraint FK72A9010B44F54FE3 foreign key (userName) references UserInfo;
alter table user_group add constraint FK72A9010BEB8E9D07 foreign key (groupName) references GroupInfo;


insert into GroupInfo (version, groupName) values (0, '___USERMANAGEMENT___');

insert into UserInfo (email, enabled, password, version, userName) values ('admin@dummydomain.com', true, '19223a7bbd7325516f069df18b50', 0, 'admin');
insert into user_group (userName, groupName) values ('admin', '___USERMANAGEMENT___');

insert into UserInfo (email, enabled, password, version, userName) values ('appuser@dummydomain.com', true, '19223a7bbd7325516f069df18b50', 0, 'appuser');
#VER 3.7.5
create sequence hibernate_sequence;
create table ModulePermissionData (roleId integer not null, primary key (roleId));
create table ServiceOperation (roleIdp integer not null, serviceTypeId varchar(255) not null, permissionValue integer not null, primary key (roleIdp, serviceTypeId));
create table ServicePermissionData (roleId integer not null, primary key (roleId));
create table Role (id bigserial, roleName varchar(255), version integer not null, primary key (id), unique (roleName));
create table group_role (groupName varchar(255) not null, roleId integer not null, primary key (groupName, roleId));
create table role_modules (role_id integer not null, modules varchar(255));
create table service_operation_permission (roleId integer not null, roleIdp integer not null, serviceTypeId varchar(255) not null, primary key (roleId, roleIdp, serviceTypeId), unique (roleIdp, serviceTypeId));
create table user_role (userName varchar(255) not null, roleId integer not null, primary key (userName, roleId));
alter table group_role add constraint FK4C707A361DA1A3C9 foreign key (roleId) references Role;
alter table group_role add constraint FK4C707A36EB8E9D07 foreign key (groupName) references GroupInfo;
alter table role_modules add constraint FK5FF978BEE47352F7 foreign key (role_id) references ModulePermissionData
alter table service_operation_permission add constraint FKEAFFB6F199BF2B2D foreign key (roleIdp, serviceTypeId) references ServiceOperation
alter table service_operation_permission add constraint FKEAFFB6F190639031 foreign key (roleId) references ServicePermissionData
alter table user_role add constraint FK143BF46A44F54FE3 foreign key (userName) references UserInfo;
alter table user_role add constraint FK143BF46A1DA1A3C9 foreign key (roleId) references Role;
alter table UserInfo add column timeZone varchar(255);
#VER 3.7.5.4
create table UserLoginHistory (userName varchar(255) not null, primary key (userName));
#VER 3.7.6.1
create table DimensionPermissionData (userId varchar(255) not null, dimensionValue bytea, primary key (userId));
#VER 3.7.7.3
#VER 4.1
alter table userinfo add lastloginhost varchar(255)
alter table userinfo add lastlogintimestamp timestamp(0)
create table userhistory (id bigserial, username varchar(255) not null, password varchar(255) not null, creationtimestamp timestamp(0) not null, primary key (id), constraint ON_USER_DELETE_CONST foreign key (username) references UserInfo on delete cascade)
create index userhistory_index on userhistory (username)
insert into userhistory (username, password, creationtimestamp) select userinfo.username, userinfo.password, localtimestamp(0) from userloginhistory inner join userinfo on userloginhistory.username=userinfo.username
#VER 4.6
create table UserFlow (id bigserial, userName varchar(255) references UserInfo(userName) ON DELETE CASCADE, displayName varchar(255), data bytea not null, version integer not null, primary key (id));
create table LaunchBoard (id bigserial, userName varchar(255) references UserInfo(userName) ON DELETE CASCADE, displayName varchar(255), universal boolean not null, data bytea not null, version integer not null, primary key (id));
#VER 4.9
alter table UserFlow alter column data type text;
alter table LaunchBoard alter column data type text;
#VER 5.0
create table application (code varchar(255) NOT NULL, name varchar(255) NOT NULL,url character varying(255) NOT NULL, primary key (code));
alter table role add column additionalinfo text;
alter table role add column applicationcode character varying(255);
alter table role ADD CONSTRAINT role_applicationcode_fkey FOREIGN KEY (applicationcode) REFERENCES application(code);
alter table role drop constraint role_rolename_key;
alter table role add constraint role_rolename_applicationcode_key UNIQUE(rolename, applicationcode);
create table user_application (userName varchar(255) not null, applicationcode varchar(255) not null, primary key (userName, applicationcode));
alter table user_application add constraint user_application_username_fkey foreign key (userName) references UserInfo;
alter table user_application add constraint user_application_applicationcode_fkey foreign key (applicationcode) REFERENCES application(code);
#VER 5.1
alter table UserInfo add column isldapuser boolean NOT NULL DEFAULT 'false';
