
/**
Tables that need to be migrated:

 - address_abroad		 // OK
 - signatory			 // OK
 - inscription_requester // OK
 - paper_deed			 // OK
 - juridical_deed		 // OK
 - person				 // OK
 - address				 // OK
 - address_line			 // OK
 */


declare @dryRun int = 0		-- 1: does not insert data into production tables; 0 does.


print '   *************************************************************************************'
print '   *                                                                                   *'
print '   * MIGRATION SCRIPT CRH --> CRF                                                      *'
print '   *                                                                                   *'
print '   *************************************************************************************'
print ''
print ''
declare @runId uniqueidentifier = newid()
print '   RUN identification: ' + convert(varchar(50), @runId)
print ''

-- Run this script on the target database
use mig_crh_staging
set nocount on





print '   PREPARE logging table'
print '   ================================================================================'
print '   This table will keep all data of successfully migrated records and will allow'
print '   you to run the migrations multiple times, eliminating the risk of migrating'
print '   the same registrations twice or more.'
print ''
print '   Upon finalising migration and decomissioning the old CRH application, this' 
print '   table may be safely removed to cleanup'
print ''

IF NOT EXISTS (SELECT 1 
           FROM INFORMATION_SCHEMA.TABLES 
           WHERE TABLE_TYPE='BASE TABLE' 
           AND TABLE_NAME='migration_crh_log' AND TABLE_SCHEMA = 'migration') 

BEGIN
	print '   Table migration.migration_crh_log created on target database'
	
	CREATE TABLE migration.migration_crh_log (
		registration_id bigint,
		resource_type varchar(100),
		resource_id bigint,
		[status] varchar(20),  -- migrated, error, corrected
		[migrated_on] datetime,
		[migrated_id] bigint,
		[message] text,
		run_uuid uniqueidentifier
	)
END 
else
begin
	print '   Table migration.migration_crh_log already existed'	
end

print ''
print '   STAGING data for address_abroad (foreign and third party penholders)'
print '   ================================================================================'
declare @addressAbroadOffset as int
select @addressAbroadOffset = 1 + (select max(address_abroad_id) from crt.address_abroad)

CREATE TABLE #mig_address_abroad
(
	registration_id bigint,
	source_address_abroad_id bigint,
	address_abroad_id bigint identity(1,1),
	country varchar(255),
	municipality varchar(255),
	house_number varchar(100),
	street varchar(255),
	type_map varchar(255)
)

DBCC CHECKIDENT ('#mig_address_abroad', RESEED, @addressAbroadOffset) WITH NO_INFOMSGS
print '   Offset id set to:        ' + convert(varchar(20), @addressAbroadOffset)

insert into #mig_address_abroad(registration_id, source_address_abroad_id, country, municipality, house_number, street, type_map)
select 
	mcr_current.registration_id as registration_id,
	ph.penholder_id as source_address_abroad_id,
	case ph.SA_AddressType
		when 'STRUCT'then ph.SA_Country
        else ph.UA_Country
    end as country,
	case ph.SA_AddressType
		when 'STRUCT' then ltrim(rtrim(ph.sa_postalcode)) + ' ' + ltrim(rtrim(ph.SA_MunicipalityName))
        else ltrim(rtrim(ph.UA_AddressLine2)) + ' ' + ltrim(rtrim(ph.UA_AddressLine3))
    end as municipality,
	case ph.SA_AddressType
		when 'STRUCT' then ph.SA_HouseNumber + ' ' + ph.SA_HouseNumberExtension
		else null
    end as house_number,
	case ph.SA_AddressType
		when 'STRUCT' then ph.SA_StreetName
        else ph.UA_AddressLine1
    end as street,
	'penholder' as type_map
from [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_current (nolock) 
inner join [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_first (nolock)
	on mcr_current.active = 1
	and mcr_current.number = mcr_first.number
	and mcr_first.version = 1
left join [mig_crh_source].[CRS].[RequestResult] rr_first (nolock)
	on rr_first.MarriageContractRegistration_Id = mcr_first.Registration_Id
	and rr_first.RequestResult_Id = (select min(requestResult_id) from [mig_crh_source].[CRS].[RequestResult] (nolock) where MarriageContractRegistration_Id = mcr_first.Registration_Id)	
left join [mig_crh_source].[CRS].[Demand] demand_first (nolock)
	on demand_first.AbstractRequest_Id = rr_first.AbstractRequest_Id
left join [mig_crh_source].[CRS].[Requester] req (nolock)
	on req.AbstractRequest_Id = rr_first.AbstractRequest_Id
	and req.Requester_Id = (select min(requester_id) from [mig_crh_source].[CRS].[Requester] (nolock) where AbstractRequest_Id = demand_first.AbstractRequest_Id)
INNER JOIN [mig_crh_source].[CRS].penholder ph  (nolock) 
    ON ph.PenHolder_Id = mcr_first.PenHolder_Id
	and ph.penholdertype = 'FOREIG'
left outer join migration.migration_crh_log mr  (nolock) 
	on mr.registration_id = mcr_current.registration_id
where isnull(mr.status, 'corrected') = 'corrected'

print '   Staged record count:     ' + convert(varchar(20), @@rowcount)


print ''
print '   STAGING data for migrating inscription_requester'
print '   ================================================================================'

declare @requesterOffset as int
select @requesterOffset = 1 + (select max(requester_id) from crt.inscription_requester)



CREATE TABLE #mig_inscription_requester
(
	registration_id bigint,
	source_requester_id bigint,
	requester_id bigint identity(1,1),
	requester_type varchar(25),
	organization_id int,
	organization_name varchar(256),
	notary_id int,
	notary_firstname varchar(100),
	notary_lastname varchar(100),
	address_abroad_id bigint,
	notary_abroad_name varchar(255),
	study_abroad_name varchar(255)
);

DBCC CHECKIDENT ('#mig_inscription_requester', RESEED, @requesterOffset) with NO_INFOMSGS

print '   Offset id set to:        ' + convert(varchar(20), @requesterOffset)




insert into #mig_inscription_requester(registration_id, source_requester_id, requester_type, organization_id, organization_name, notary_id, notary_firstname, notary_lastname, address_abroad_id, notary_abroad_name, study_abroad_name)
select 
	mcr_current.Registration_Id as registration_id,
	req.requester_id as requester_id,
	CASE req.RequesterType
      WHEN 'MOROAS' THEN 'REGISTER_ABROAD_MANAGER'  
      WHEN 'FRNB' THEN 'FEDNOT'
      WHEN 'FPSFA' then 'FOREIGN_AFFAIRS'
      ELSE req.RequesterType
    END	as requester_type,
	CASE
		WHEN req.requestertype = 'FRNB' THEN isnull(demand_first.creatorstudyid, 214422)
        ELSE req.studyreference
    END	as organization_id,
	CASE req.RequesterType
		WHEN 'FRNB' THEN 'FedNot'
		WHEN 'ADSN' THEN 'ADSN'
		WHEN 'FPSFA' THEN
			case demand_first.CertificateLanguageRequested
				when 'nl' then 'FOD Buitenlandse zaken'
				when 'fr' then 'SPF Affaires Etrangères'
			end
		ELSE req.organizationname
    END	as organization_name,
	isnull(req.PersonReference, ph.personId) as notary_id,

	CASE req.requestertype
		WHEN 'POLICE' THEN req.firstname
		WHEN 'MOROAS' THEN req.[name]
		WHEN 'THIPAR' THEN req.[name]
		WHEN 'STUDY' THEN isnull(req.firstname, ph.FirstName)
		WHEN 'FRNB' THEN ph.FirstName
    END as notary_first_name,
	CASE req.requestertype
		WHEN 'POLICE' THEN req.LastName
		WHEN 'MOROAS' THEN ''
		WHEN 'THIPAR' THEN ''
		WHEN 'STUDY' THEN isnull(req.lastname, ph.LastName)
		WHEN 'FRNB' THEN ph.LastName
    END	as notary_last_name,
	maa.address_abroad_id as address_abroad_id, 
	null as notary_abroad_name, 
	null as study_abroad_name 

from [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_current (nolock) 
inner join [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_first (nolock)
	on mcr_current.active = 1
	and mcr_current.number = mcr_first.number
	and mcr_first.version = 1
left join [mig_crh_source].[CRS].[RequestResult] rr_first (nolock)
	on rr_first.MarriageContractRegistration_Id = mcr_first.Registration_Id
	and rr_first.RequestResult_Id = (select min(requestResult_id) from [mig_crh_source].[CRS].[RequestResult] (nolock) where MarriageContractRegistration_Id = mcr_first.Registration_Id)	
left join [mig_crh_source].[CRS].[Demand] demand_first (nolock)
	on demand_first.AbstractRequest_Id = rr_first.AbstractRequest_Id
left join [mig_crh_source].[CRS].[Requester] req (nolock)
	on req.AbstractRequest_Id = rr_first.AbstractRequest_Id
	and req.Requester_Id = (select min(requester_id) from [mig_crh_source].[CRS].[Requester] (nolock) where AbstractRequest_Id = demand_first.AbstractRequest_Id)
LEFT JOIN [mig_crh_source].[CRS].penholder ph  (nolock) 
    ON ph.PenHolder_Id = mcr_first.PenHolder_Id
LEFT JOIN #mig_address_abroad maa
	on maa.registration_id = mcr_current.registration_id
	and maa.source_address_abroad_id = ph.penholder_id
left join migration.migration_crh_log mr  (nolock) 
	on mr.registration_id = mcr_current.registration_id
where isnull(mr.status, 'corrected') = 'corrected'

print '   Staged record count:     ' + convert(varchar(20), @@rowcount)

print ''
print '   STAGING data for migrating signatory'
print '   ================================================================================'
declare @signatoryOffset as int
select @signatoryOffset = 1 + (select max(signatory_id) from crt.signatory)

CREATE TABLE #mig_signatory
(
	registration_id bigint,
	source_signatory_id bigint,
	signatory_id bigint identity(1,1),
	organization_id int,
	organization_name varchar(256),
	notary_id int,
	notary_firstname varchar(100),
	notary_lastname varchar(100),
	signatory_type varchar(100),
	address_abroad_id bigint,
	notary_abroad_name varchar(255),
	study_abroad_name varchar(255),
	nrn varchar(11)
)
DBCC CHECKIDENT ('#mig_signatory', RESEED, @signatoryOffset) with NO_INFOMSGS
print '   Offset id set to:        ' + convert(varchar(20), @signatoryOffset)

insert into #mig_signatory(registration_id, source_signatory_id, organization_id, organization_name, notary_id, notary_firstname, notary_lastname, signatory_type, address_abroad_id, notary_abroad_name, study_abroad_name, nrn)
select 
	mcr_current.registration_id,
	ph.PenHolder_Id as source_signatory_id,
	ph.OrganizationId as organization_id,
	ph.OrganizationName as organization_name,
	ph.personId as notary_id,
	ph.firstname as notary_firstname,
	ph.lastname as notary_lastname,
	case ph.PenHolderType 
		when 'NOT' then 'STUDY'
        when 'FOREIG' then 'ABROAD'
    else penholdertype end as signatory_type,
	null as address_abroad_id,
	null as notary_abroad_name,
	case ph.penholdertype
		when 'FOREIG' then ph.name
		else null
	end as study_abroad_name,
	null as nrn
from [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_current (nolock) 
inner join [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_first (nolock)
	on mcr_current.active = 1
	and mcr_current.number = mcr_first.number
	and mcr_first.version = 1
inner join [mig_crh_source].[CRS].penholder ph  (nolock) 
	on ph.penholder_id = mcr_current.PenHolder_Id
left outer join migration.migration_crh_log mr (nolock) 
	on mr.registration_id = mcr_current.registration_id
where isnull(mr.status, 'corrected') = 'corrected'

print '   Staged record count:     ' + convert(varchar(20), @@rowcount)

print ''
print '   STAGING data for migrating paper_deed'
print '   ================================================================================'

declare @paperDeedOffset as int
select @paperDeedOffset = 1 + (select max(paper_deed_id) from crt.paper_deed)

declare @expectedNumberOfPaperDeeds as int
select @expectedNumberOfPaperDeeds = count(*) from [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_current (nolock) 
inner join [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_first (nolock)
	on mcr_current.active = 1
	and mcr_current.number = mcr_first.number
	and mcr_first.version = 1

CREATE TABLE #mig_paper_deed
(
	registration_id bigint,
	source_paper_deed_id bigint,
	paper_deed_id bigint identity(1,1),
	status_id tinyint,
	deed_date date,
	dossier_reference varchar(50),
	repertorium_number varchar(50),
	request_date date,
	signatory_id bigint,
	created_on datetime,
	updated_on datetime,
	updated_by_user_name varchar(100),
	created_by_organization_name varchar(100),
	created_by_organization_id bigint,
	created_by_user_name varchar(100),
	created_by_user_id bigint,
	requester_id bigint,
	updated_by_organization_name varchar(100),
	updated_by_organization_id bigint,
	updated_by_user_id bigint,
	import_state varchar(20),
	import_result varchar(20),
	nap_id varchar(50)
);

DBCC CHECKIDENT ('#mig_paper_deed', RESEED, @paperDeedOffset) with NO_INFOMSGS
print '   Offset id set to:        ' + convert(varchar(20), @paperDeedOffset)


insert into #mig_paper_deed( registration_id, source_paper_deed_id, status_id, deed_date, dossier_reference, repertorium_number, request_date, signatory_id, created_on, updated_on, updated_by_user_name, created_by_organization_name, created_by_organization_id, created_by_user_name, created_by_user_id, requester_id, updated_by_organization_name, updated_by_organization_id, updated_by_user_id, import_state, import_result, nap_id)
select 
	mcr_current.registration_id as registration_id,
	mcr_current.registration_id as paper_deed_id,
	case mcr_current.status when 'VAL' then 0 when 'CAN' then 1 else null end as status_id,
	mcr_current.ActDate as deed_date,
	mcr_current.DossierReference as dossier_reference,
	mcr_current.RepertoryNumber as repertorium_number,
	isnull(original_demand_first.requestreceiveddate, isnull(demand_first.requestreceiveddate, mcr_first.DateTimeRegistration)) as request_date,
	msig.signatory_id as signatory_id,
	mcr_first.DateTimeRegistration as created_on,
	mcr_current.DateTimeRegistration as updated_on,
	demand_current.CreatorName as updated_by_user_name,
	msig.organization_name as created_by_organization_name, 
	demand_first.CreatorStudyId as created_by_organization_id,
	demand_first.CreatorName as created_by_user_name,
	demand_first.CreatorPersonId as created_by_user_id,
	reqmap.requester_id as requester_id, 
	msig.organization_name as updated_by_organization_name,
	demand_current.CreatorStudyId as updated_by_organization_id,
	demand_current.CreatorPersonId as updated_by_user_id,
	null as import_state,
	null as import_result,
	null as nap_id--,
from [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_current (nolock) 
inner join [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_first (nolock)
	on mcr_current.active = 1
	and mcr_current.number = mcr_first.number
	and mcr_first.version = 1
inner join #mig_signatory msig (nolock) 
	on msig.source_signatory_id = mcr_current.PenHolder_Id  
	and msig.registration_id = mcr_current.registration_id
left join [mig_crh_source].[CRS].[RequestResult] rr_first (nolock)
	on rr_first.MarriageContractRegistration_Id = mcr_first.Registration_Id
	and rr_first.RequestResult_Id = (select min(requestResult_id) from [mig_crh_source].[CRS].[RequestResult] (nolock) where MarriageContractRegistration_Id = mcr_first.Registration_Id)
left join [mig_crh_source].[CRS].[RequestResult] rr_current (nolock) 
	on rr_current.MarriageContractRegistration_Id = mcr_current.Registration_Id
	and rr_current.RequestResult_Id = (select max(requestResult_id) from [mig_crh_source].[CRS].[RequestResult] (nolock) where MarriageContractRegistration_Id = mcr_current.Registration_Id)
left join [mig_crh_source].[CRS].[Demand] demand_first (nolock)
	on demand_first.AbstractRequest_Id = rr_first.AbstractRequest_Id
left join [mig_crh_source].[CRS].[Demand] original_demand_first (nolock)
	on original_demand_first.AbstractRequest_Id = demand_first.OriginalDemand_Id
left join [mig_crh_source].[CRS].[Demand] demand_current (nolock)
	on demand_current.AbstractRequest_Id = rr_current.AbstractRequest_Id
left join [mig_crh_source].[CRS].[Demand] original_demand_current (nolock)
	on original_demand_current.AbstractRequest_Id = demand_current.OriginalDemand_Id
left join [mig_crh_source].[CRS].[Requester] req (nolock)
	on req.AbstractRequest_Id = rr_first.AbstractRequest_Id
	and req.Requester_Id = (select min(requester_id) from [mig_crh_source].[CRS].[Requester] (nolock) where AbstractRequest_Id = demand_first.AbstractRequest_Id)
left join #mig_inscription_requester reqmap (nolock) 
	on req.Requester_Id = reqmap.source_requester_id
left outer join migration.migration_crh_log mr (nolock) 
	on mr.registration_id = mcr_current.registration_id
where isnull(mr.status, 'corrected') = 'corrected'

print '   Staged record count:     ' + convert(varchar(20), @@rowcount) + ' / ' + convert(varchar(20), @expectedNumberOfPaperDeeds) + ' expected'


print ''
print '   STAGING data for migrating juridical_deed'
print '   ================================================================================'
declare @juridicalDeedOffset as int
select @juridicalDeedOffset = 1 + (select max(juridical_deed_id) from crt.juridical_deed)

CREATE TABLE #mig_juridical_deed
(
	registration_id bigint,
	source_juridical_deed_id bigint,
	juridical_deed_id bigint identity(1,1),
	juridical_deed_number varchar(100),
	paper_deed_id bigint,
	status_id tinyint,
	registration_type_id tinyint,
	doc_contents_id tinyint,
	to_invoice bit,
	created_on datetime,
	updated_on datetime,
	updated_by_user_name varchar(100),
	created_by_organization_name varchar(100),
	created_by_organization_id bigint,
	created_by_user_name varchar(100),
	created_by_user_id bigint,
	updated_by_organization_name varchar(100),
	updated_by_organization_id bigint,
	updated_by_user_id bigint,
	operation_type_id tinyint,
	register varchar(3),
	description varchar(100),
	keeps_own_document bit,
	previous_doc_contents_id tinyint,
	publish_language varchar(2),
	to_publish bit
);

DBCC CHECKIDENT ('#mig_juridical_deed', RESEED, @juridicalDeedOffset) with NO_INFOMSGS
print '   Offset id set to:        ' + convert(varchar(20), @juridicalDeedOffset)

INSERT INTO #mig_juridical_deed( registration_id, source_juridical_deed_id,juridical_deed_number ,paper_deed_id ,status_id ,registration_type_id ,doc_contents_id ,to_invoice ,created_on ,updated_on ,updated_by_user_name ,created_by_organization_name ,created_by_organization_id ,created_by_user_name ,created_by_user_id ,updated_by_organization_name ,updated_by_organization_id ,updated_by_user_id ,operation_type_id ,register ,[description] ,keeps_own_document ,previous_doc_contents_id ,publish_language ,to_publish )
SELECT 
	mcr_current.registration_id as registration_id,
	mcr_current.registration_id as source_juridical_deed_id,
	mcr_current.number as juridical_deed_number ,
	mpd.paper_deed_id as paper_deed_id ,
	0 as status_id ,
	case mcr_current.RegistrationSubType
		WHEN 'HANTCON' then 13
		WHEN 'HJUDGMNT' then 16
		WHEN 'HLEGCOH' then 12
		WHEN 'HMARR'  then 14
		WHEN 'HMODDEED' then 15
	end as registration_type_id ,  
	case WHEN mcr_current.RegistrationSubType IN ('HMARR', 'HMODDEED') THEN
		CASE mcr_current.MarriageRegimeCode
			WHEN 'CONVCOMM' THEN 3
			WHEN 'FORRIGH' THEN 8
			WHEN 'JSEPGOODS' THEN null -- ToDo: validate mapping!
			WHEN 'LEGAL' THEN 2
			WHEN 'LIMMACO' THEN null -- ToDo: validate mapping!
			WHEN 'ORB1976' THEN 1
			WHEN 'OTHER' THEN null -- ToDo: validate mapping!
			WHEN 'SEPGOODSADD' THEN 7
			WHEN 'SEPGOODSCL' THEN 6
			WHEN 'SEPGOODSP' THEN 5   
			WHEN 'UNICOMM' THEN 4
			ELSE null
		END
	ELSE null
	end as doc_contents_id ,
	1 as to_invoice ,
	mpd.created_on as created_on ,
	mpd.updated_on as updated_on ,
	mpd.updated_by_user_name as updated_by_user_name ,
	mpd.created_by_organization_name as created_by_organization_name ,
	mpd.created_by_organization_id as created_by_organization_id ,
	mpd.created_by_user_name as created_by_user_name ,
	mpd.created_by_user_id as created_by_user_id ,
	mpd.updated_by_organization_name as updated_by_organization_name ,
	mpd.updated_by_organization_id as updated_by_organization_id ,
	mpd.updated_by_user_id as updated_by_user_id ,
	0 as operation_type_id ,
	'CRH' as register ,
	null as description , -- only used for judgments and decrees
	NULL as keeps_own_document , -- only applicable for CRT
	case WHEN mcr_current.RegistrationSubType IN ('HMARR', 'HMODDEED') THEN
		CASE mcr_current.PrevMarriageRegimeCode
			WHEN 'CONVCOMM' THEN 3
			WHEN 'FORRIGH' THEN 8
			WHEN 'JSEPGOODS' THEN null -- ToDo: validate mapping!
			WHEN 'LEGAL' THEN 2
			WHEN 'LIMMACO' THEN null -- ToDo: validate mapping!
			WHEN 'ORB1976' THEN 1
			WHEN 'OTHER' THEN null -- ToDo: validate mapping!
			WHEN 'SEPGOODSADD' THEN 7
			WHEN 'SEPGOODSCL' THEN 6
			WHEN 'SEPGOODSP' THEN 5   
			WHEN 'UNICOMM' THEN 4
			ELSE null
		END
	ELSE null
	end as previous_doc_contents_id ,
	mcr_current.BelgianJournalPublicationLanguage as publish_language , -- This will probably need to change after story of publications!
	mcr_current.BelgianJournalPublicationRequested as to_publish -- This will probably need to change after story of pulications!
FROM [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_current (nolock) 
inner join [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_first (nolock)
	on mcr_current.active = 1
	and mcr_current.number = mcr_first.number
	and mcr_first.Version = 1
inner join #mig_paper_deed mpd (nolock) 
	on mpd.source_paper_deed_id = mcr_current.registration_id
left outer join migration.migration_crh_log mr (nolock) 
	on mr.registration_id = mcr_current.registration_id
where isnull(mr.status, 'corrected') = 'corrected'
print '   Staged record count:     ' + convert(varchar(20), @@rowcount)
	
print ''
print '   STAGING data for migrating person'
print '   ================================================================================'
declare @personOffset as int
select @personOffset = 1 + (select max(person_id) from crt.person)

CREATE TABLE #mig_person
(
	registration_id bigint,
	source_person_id bigint,
	person_id bigint identity(1,1),
	person_type varchar(20),
	nrn varchar(11),
	first_name varchar(256),
	last_name varchar(256),
	birth_date varchar(10),
	birth_country_code varchar(256),
	birth_country_description varchar(256),
	birth_municipality_nis_code varchar(256),
	birth_municipality_name varchar(256),
	juridical_deed_id bigint,
	person_role_id tinyint,
	name varchar(256),
	enterprise_number varchar(10),
	juridical_form varchar(50),
);

DBCC CHECKIDENT ('#mig_person', RESEED, @personOffset) with NO_INFOMSGS
print '   Offset id set to:        ' + convert(varchar(20), @personOffset)

insert into #mig_person(registration_id, source_person_id, person_type, nrn, first_name, last_name, birth_date, birth_country_code, birth_country_description, birth_municipality_nis_code, birth_municipality_name, juridical_deed_id, person_role_id, name, enterprise_number, juridical_form)
select 
	mjd.registration_id as registration_id,
	p.involvedparty_id as source_person_id ,
	'NATURAL_PERSON' as person_type ,
	p.identificationnumber as nrn ,
	p.firstNames as first_name ,
	p.lastNames as last_name ,
	CASE WHEN p.[BirthDay] IS NULL THEN RIGHT('0' + CONVERT(VARCHAR, p.[BirthYear]), 2)
    ELSE
        RIGHT('0' + CONVERT(VARCHAR, p.[BirthDay]), 2) + '/'
        + RIGHT('0' + CONVERT(VARCHAR, p.[BirthMonth]), 2) + '/'
        + RIGHT('000' + CONVERT(VARCHAR, p.[BirthYear]), 4)
    END as birth_date ,
	p.birthCountry as birth_country_code ,
	c.name_nl as birth_country_description ,
	p.[BirthMunicipalityCode] as birth_municipality_nis_code ,
	p.[BirthMunicipalityName] as birth_municipality_name ,
	mjd.juridical_deed_id as juridical_deed_id ,
	4 as person_role_id ,
	null as name ,
	null as enterprise_number ,
	null as juridical_form
from #mig_juridical_deed mjd (nolock) 
inner join [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_current (nolock) 
	on mcr_current.registration_id = mjd.source_juridical_deed_id
inner join [mig_crh_source].[CRS].InvolvedParty p (nolock) 
	on p.involvedparty_id = mcr_current.party1_id
	or p.involvedparty_id = mcr_current.party2_id
left outer join [mig_crh_source].[CRS].Country c (nolock) 
	on c.code = p.birthCountry
left outer join migration.migration_crh_log mr (nolock) 
	on mr.registration_id = mcr_current.registration_id
where isnull(mr.status, 'corrected') = 'corrected'

print '   Staged record count:     ' + convert(varchar(20), @@rowcount)

print ''
print '   STAGING data for migrating address'
print '   ================================================================================'
declare @addressOffset as int
select @addressOffset = 1 + (select max(address_id) from crt.address)



CREATE TABLE #mig_address
(
	registration_id bigint,
	source_address_id bigint,
	address_id bigint identity(1,1),
	person_id bigint,
	country_code varchar(2),
	country_description varchar(256),
	municipality_nis_code varchar(5),
	municipality_name varchar(100),
	house_number varchar(100),
	postal_box varchar(100),
	postal_code varchar(15),
	street_name varchar(255),
	type varchar(20)
);

DBCC CHECKIDENT ('#mig_address', RESEED, @addressOffset) with NO_INFOMSGS
print '   Offset id set to:        ' + convert(varchar(20), @addressOffset)

insert into #mig_address(registration_id, source_address_id ,person_id ,country_code ,country_description ,municipality_nis_code ,municipality_name ,house_number ,postal_box ,postal_code ,street_name ,type )
select
	mjd.registration_id, 
	p.involvedparty_id as source_address_id,
	mp.person_id person_id,
	case when p.SA_AddressType = 'STRUCT' then p.SA_Country else p.UA_Country end as country_code,
	c.name_nl as country_description,
	p.SA_MunicipalityCode as municipality_nis_code,
    p.SA_MunicipalityName as municipality_name,
    p.SA_HouseNumber as house_number,
    p.SA_HouseNumberExtension as postal_box,
    p.SA_PostalCode as postal_code,
    p.SA_StreetName as street_name,
	case when p.SA_AddressType = 'STRUCT' then 'STRUCTURED' else 'UNSTRUCTURED' end as [type] 
from #mig_juridical_deed mjd (nolock) 
inner join [mig_crh_source].[CRS].[MarriageContractRegistration] mcr_current (nolock) 
	on mcr_current.registration_id = mjd.source_juridical_deed_id
inner join [mig_crh_source].[CRS].InvolvedParty p (nolock) 
	on p.involvedparty_id = mcr_current.party1_id
	or p.involvedparty_id = mcr_current.party2_id
left join [mig_crh_source].[CRS].Country c (nolock) 
	on c.code = case when p.SA_AddressType = 'STRUCT' then p.SA_Country else p.UA_Country end
inner join #mig_person mp (nolock) 
	on mp.source_person_id = p.InvolvedParty_Id
	and mp.registration_id = mcr_current.registration_id
left outer join migration.migration_crh_log mr (nolock) 
	on mr.registration_id = mcr_current.registration_id
where isnull(mr.status, 'corrected') = 'corrected'

print '   Staged record count:     ' + convert(varchar(20), @@rowcount)

print ''
print '   STAGING data for migrating address_lines'
print '   ================================================================================'
declare @addressLineOffset as int
select @addressLineOffset = 1 + (select max(address_line_id) from crt.address_line)

CREATE TABLE #mig_address_line
(
	registration_id bigint,
	source_address_line_id bigint,
	address_line_id bigint identity(1,1),
	lines varchar(256),
	address_id bigint
);

DBCC CHECKIDENT ('#mig_address_line', RESEED, @addressLineOffset) with NO_INFOMSGS
print '   Offset id set to:        ' + convert(varchar(20), @addressLineOffset)

insert into #mig_address_line(registration_id, source_address_line_id, lines, address_id)
select 
	ma.registration_id,
	1000000000 + p.InvolvedParty_Id as source_address_line_id,
	p.UA_AddressLine1, 
	ma.address_id
from #mig_address ma (nolock) 
inner join [mig_crh_source].[CRS].InvolvedParty p (nolock) 
	on p.involvedparty_id = ma.source_address_id
	and isnull(p.SA_AddressType, 'UNSTRUCTURED') <> 'STRUCT'
	and p.UA_AddressLine1 is not null
left outer join migration.migration_crh_log mr (nolock) 
	on mr.registration_id = ma.registration_id
where isnull(mr.status, 'corrected') = 'corrected'
union 
select 
	ma.registration_id,
	2000000000 + p.InvolvedParty_Id as source_address_line_id,
	p.UA_AddressLine2, 
	ma.address_id
from #mig_address ma (nolock) 
inner join [mig_crh_source].[CRS].InvolvedParty p (nolock) 
	on p.involvedparty_id = ma.source_address_id
	and isnull(p.SA_AddressType, 'UNSTRUCTURED') <> 'STRUCT'
	and p.UA_AddressLine2 is not null
left outer join migration.migration_crh_log mr (nolock) 
	on mr.registration_id = ma.registration_id
where isnull(mr.status, 'corrected') = 'corrected'
union 
select 
	ma.registration_id,
	3000000000 + p.InvolvedParty_Id as source_address_line_id,
	p.UA_AddressLine3, 
	ma.address_id
from #mig_address ma (nolock) 
inner join [mig_crh_source].[CRS].InvolvedParty p (nolock) 
	on p.involvedparty_id = ma.source_address_id
	and isnull(p.SA_AddressType, 'UNSTRUCTURED') <> 'STRUCT'
	and p.UA_AddressLine3 is not null
left outer join migration.migration_crh_log mr (nolock) 
	on mr.registration_id = ma.registration_id
where isnull(mr.status, 'corrected') = 'corrected'
union 
select 
	ma.registration_id,
	4000000000 + p.InvolvedParty_Id as source_address_line_id,
	p.UA_AddressLine4, 
	ma.address_id
from #mig_address ma (nolock) 
inner join [mig_crh_source].[CRS].InvolvedParty p (nolock) 
	on p.involvedparty_id = ma.source_address_id
	and isnull(p.SA_AddressType, 'UNSTRUCTURED') <> 'STRUCT'
	and p.UA_AddressLine4 is not null
left outer join migration.migration_crh_log mr (nolock) 
	on mr.registration_id = ma.registration_id
where isnull(mr.status, 'corrected') = 'corrected'
union 
select 
	ma.registration_id,
	5000000000 + p.InvolvedParty_Id as source_address_line_id,
	p.UA_AddressLine5, 
	ma.address_id
from #mig_address ma (nolock) 
inner join [mig_crh_source].[CRS].InvolvedParty p (nolock) 
	on p.involvedparty_id = ma.source_address_id
	and isnull(p.SA_AddressType, 'UNSTRUCTURED') <> 'STRUCT'
	and p.UA_AddressLine5 is not null
left outer join migration.migration_crh_log mr (nolock) 
	on mr.registration_id = ma.registration_id
where isnull(mr.status, 'corrected') = 'corrected'
order by address_id, source_address_line_id
print '   Staged record count:     ' + convert(varchar(20), @@rowcount)

-----------------------------------------------------------------------------------------
--                                                                                     --
-- RUN VALIDATIONS ON STAGED TABLES                                                    --
--                                                                                     --
-----------------------------------------------------------------------------------------





	
begin tran


print ''
print '   PROMOTE validated data to PRODUCTION for address_abroad'
print '   ================================================================================'

SET IDENTITY_INSERT [CRT].address_abroad on 

insert into [CRT].address_abroad(address_abroad_id, country, municipality, house_number, street, type_map)
select address_abroad_id, country, municipality, house_number, street, type_map
from #mig_address_abroad m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Promoted record count:   ' + convert(varchar(20), @@rowcount)

insert into migration.migration_crh_log(registration_id, resource_type, resource_id, status, migrated_on, migrated_id, message, run_uuid)
select m.registration_id, 'address_abroad', source_address_abroad_id, 'migrated', getdate(), address_abroad_id, 'Successfully migrated', @runId
from #mig_address_abroad m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Logged record count  :   ' + convert(varchar(20), @@rowcount)

SET IDENTITY_INSERT [CRT].address_abroad off

print ''
print '   PROMOTE validated data to PRODUCTION for inscription_requester'
print '   ================================================================================'

SET IDENTITY_INSERT [CRT].inscription_requester on 

insert into [CRT].inscription_requester(requester_id, requester_type, organization_id, organization_name, notary_id, notary_firstname, notary_lastname, address_abroad_id, notary_abroad_name, study_abroad_name)
select requester_id, requester_type, organization_id, organization_name, notary_id, notary_firstname, notary_lastname, address_abroad_id, notary_abroad_name, study_abroad_name
from #mig_inscription_requester m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Promoted record count:   ' + convert(varchar(20), @@rowcount)

insert into migration.migration_crh_log(registration_id, resource_type, resource_id, status, migrated_on, migrated_id, message, run_uuid)
select m.registration_id, 'inscription_requester', source_requester_id, 'migrated', getdate(), requester_id, 'Successfully migrated', @runId
from #mig_inscription_requester m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Logged record count  :   ' + convert(varchar(20), @@rowcount)


SET IDENTITY_INSERT [CRT].inscription_requester off

print ''
print '   PROMOTE validated data to PRODUCTION for signatory'
print '   ================================================================================'

SET IDENTITY_INSERT [CRT].signatory on 

insert into [CRT].signatory(signatory_id, organization_id, organization_name, notary_id, notary_firstname, notary_lastname, signatory_type, address_abroad_id, notary_abroad_name, study_abroad_name, nrn)
select signatory_id, organization_id, organization_name, notary_id, notary_firstname, notary_lastname, signatory_type, address_abroad_id, notary_abroad_name, study_abroad_name, nrn
from #mig_signatory m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Promoted record count:   ' + convert(varchar(20), @@rowcount)

SET IDENTITY_INSERT [CRT].signatory off

insert into migration.migration_crh_log(registration_id, resource_type, resource_id, status, migrated_on, migrated_id, message, run_uuid)
select m.registration_id, 'signatory', source_signatory_id, 'migrated', getdate(), signatory_id, 'Successfully migrated', @runId
from #mig_signatory m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Logged record count  :   ' + convert(varchar(20), @@rowcount)



print ''
print '   PROMOTE validated data to PRODUCTION for paper_deed'
print '   ================================================================================'

SET IDENTITY_INSERT [CRT].paper_deed on

insert into [CRT].paper_deed(paper_deed_id, status_id, deed_date, dossier_reference, repertorium_number, request_date, signatory_id, created_on, updated_on, updated_by_user_name, created_by_organization_name, created_by_organization_id, created_by_user_name, created_by_user_id, requester_id, updated_by_organization_name, updated_by_organization_id, updated_by_user_id, import_state, import_result, nap_id)
select paper_deed_id, status_id, deed_date, dossier_reference, repertorium_number, request_date, signatory_id, created_on, updated_on, updated_by_user_name, created_by_organization_name, created_by_organization_id, created_by_user_name, created_by_user_id, requester_id, updated_by_organization_name, updated_by_organization_id, updated_by_user_id, import_state, import_result, nap_id
from #mig_paper_deed m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Promoted record count:   ' + convert(varchar(20), @@rowcount)

SET IDENTITY_INSERT [CRT].paper_deed off

insert into migration.migration_crh_log(registration_id, resource_type, resource_id, status, migrated_on, migrated_id, message, run_uuid)
select m.registration_id, 'paper_deed', source_paper_deed_id, 'migrated', getdate(), paper_deed_id, 'Successfully migrated', @runId
from #mig_paper_deed m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Logged record count  :   ' + convert(varchar(20), @@rowcount)

print ''
print '   PROMOTE validated data to PRODUCTION for juridical_deed'
print '   ================================================================================'

SET IDENTITY_INSERT [CRT].juridical_deed on
	
insert into [CRT].juridical_deed(juridical_deed_id,juridical_deed_number ,paper_deed_id ,status_id ,registration_type_id ,doc_contents_id ,to_invoice ,created_on ,updated_on ,updated_by_user_name ,created_by_organization_name ,created_by_organization_id ,created_by_user_name ,created_by_user_id ,updated_by_organization_name ,updated_by_organization_id ,updated_by_user_id ,operation_type_id ,register ,[description] ,keeps_own_document ,previous_doc_contents_id ,publish_language ,to_publish )
select juridical_deed_id,juridical_deed_number ,paper_deed_id ,status_id ,registration_type_id ,doc_contents_id ,to_invoice ,created_on ,updated_on ,updated_by_user_name ,created_by_organization_name ,created_by_organization_id ,created_by_user_name ,created_by_user_id ,updated_by_organization_name ,updated_by_organization_id ,updated_by_user_id ,operation_type_id ,register ,[description] ,keeps_own_document ,previous_doc_contents_id ,publish_language ,to_publish 
from #mig_juridical_deed m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Promoted record count:   ' + convert(varchar(20), @@rowcount)

SET IDENTITY_INSERT [CRT].juridical_deed off

insert into migration.migration_crh_log(registration_id, resource_type, resource_id, status, migrated_on, migrated_id, message, run_uuid)
select m.registration_id, 'juridical_deed', source_juridical_deed_id, 'migrated', getdate(), juridical_deed_id, 'Successfully migrated', @runId
from #mig_juridical_deed m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Logged record count  :   ' + convert(varchar(20), @@rowcount)

print ''
print '   PROMOTE validated data to PRODUCTION for person'
print '   ================================================================================'

SET IDENTITY_INSERT [CRT].person on
	
insert into [CRT].person(person_id, person_type, nrn, first_name, last_name, birth_date, birth_country_code, birth_country_description, birth_municipality_nis_code, birth_municipality_name, juridical_deed_id, person_role_id, name, enterprise_number, juridical_form )
select person_id, person_type, nrn, first_name, last_name, birth_date, birth_country_code, birth_country_description, birth_municipality_nis_code, birth_municipality_name, juridical_deed_id, person_role_id, name, enterprise_number, juridical_form 
from #mig_person m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Promoted record count:   ' + convert(varchar(20), @@rowcount)

SET IDENTITY_INSERT [CRT].person off

insert into migration.migration_crh_log(registration_id, resource_type, resource_id, status, migrated_on, migrated_id, message, run_uuid)
select m.registration_id, 'person', source_person_id, 'migrated', getdate(), person_id, 'Successfully migrated', @runId
from #mig_person m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Logged record count  :   ' + convert(varchar(20), @@rowcount)



print ''
print '   PROMOTE validated data to PRODUCTION for address'
print '   ================================================================================'

SET IDENTITY_INSERT [CRT].address on
	
insert into [CRT].address(address_id ,person_id ,country_code ,country_description ,municipality_nis_code ,municipality_name ,house_number ,postal_box ,postal_code ,street_name ,type )
select address_id ,person_id ,country_code ,country_description ,municipality_nis_code ,municipality_name ,house_number ,postal_box ,postal_code ,street_name ,type
from #mig_address m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Promoted record count:   ' + convert(varchar(20), @@rowcount)

SET IDENTITY_INSERT [CRT].address off

insert into migration.migration_crh_log(registration_id, resource_type, resource_id, status, migrated_on, migrated_id, message, run_uuid)
select m.registration_id, 'address', source_address_id, 'migrated', getdate(), address_id, 'Successfully migrated', @runId
from #mig_address m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Logged record count  :   ' + convert(varchar(20), @@rowcount)

print ''
print '   PROMOTE validated data to PRODUCTION for address_line'
print '   ================================================================================'

SET IDENTITY_INSERT [CRT].address_line on
	
insert into [CRT].address_line(address_line_id, lines, address_id )
select address_line_id, lines, address_id
from #mig_address_line m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Promoted record count:   ' + convert(varchar(20), @@rowcount)

SET IDENTITY_INSERT [CRT].address_line off

insert into migration.migration_crh_log(registration_id, resource_type, resource_id, status, migrated_on, migrated_id, message, run_uuid)
select m.registration_id, 'address_line', source_address_line_id, 'migrated', getdate(), address_line_id, 'Successfully migrated', @runId
from #mig_address_line m
left outer join migration.migration_crh_log mr
	on mr.registration_id = m.registration_id
	and (mr.status <> 'migrated' or mr.run_uuid <> @runId)
where isnull(mr.status, 'corrected') = 'corrected'

print '   Logged record count  :   ' + convert(varchar(20), @@rowcount)


print ''
print '   CLEANUP dropping temporary tables'
print '   ================================================================================'

-- Drop temporary tables
drop table #mig_inscription_requester
print 'OK #mig_inscription_requester dropped'
drop table #mig_paper_deed
print 'OK #mig_paper_deed dropped'
drop table #mig_juridical_deed
print 'OK #mig_juridical_deed dropped'
drop table #mig_person
print 'OK #mig_person dropped'
drop table #mig_address
print 'OK #mig_address dropped'
drop table #mig_address_line
print 'OK #mig_address_line dropped'
drop table #mig_signatory
print 'OK #mig_signatory dropped'

if @dryRun = 0 
begin
	commit
	print 'OK  TRANSACTION COMMITTED.  MIGRATION COMPLETED'
end
else
begin
	rollback
	print '!!  TRANSACTION ROLLED BACK.  MIGRATION DID NOT COMPLETE - DRY RUN ONLY'
end






