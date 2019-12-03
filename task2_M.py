from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import requests
import json
import pandas as pd
import random
import numpy as np
from pandas.io.json import json_normalize


## Main Function
def semanticCheck(col):
    labels = ["Business Name", 'School Levels','Park/Playground','Building Classification','College/University names',\
              'Phone number', 'Address', 'Street Name', 'City', 'Neighborhood','LAT/LON coordinates'\
              'Zip code', 'Borough','School Name','Color','Car Make','City agency'\
              'Areas of study', 'Subjects in school',"Person Name",  \
              'Websites','Vehicle Type', 'Type of location']

    checkEach = [checkBusinessName(col),checkSchoolLevel(col),checkStreetName(col),checkParkandPlayground(col),checkCityAgencies(col),checkBuildingType(col)]
    result=[]
    for i in range(0, len(checkEach)):
        if(checkEach[i]==True):
            result.append(labels[i])

    return result





def generalCheck(column, list):
    size = len(column)
    sampleSize = size * 0.1
    check = sampleSize
    cnt = 0

    while check > 0:
        rand = random.randint(0, size - 1)
        ele = str(column[rand])
        flag = False
        for s in list:
            if (fuzz.partial_ratio(ele.lower(), s.lower()) > 70):
                flag = True
                break
        #print(ele, "  ", fuzz.partial_ratio(ele.lower(), s.lower()))
        if (flag):
            cnt += 1
        check -= 1

    if ((cnt / sampleSize) > 0.5):
        return True
    else:
        return False


def checkBusinessName(column):
    response = requests.get("https://data.ny.gov/resource/n9v6-gdp6.json")
    data = response.json()
    businessName = [item['current_entity_name'] for item in data]
    return generalCheck(column, businessName)


def checkSchoolLevel(column):
    schoolLevels = np.asarray(['K-12','K-11','K-10','K-9','K-8','K-7','K-6','K-5','K-4','K-3','K-2','K-1','Elementry','Middle school','high school','college','grade'])
    return generalCheck(column, schoolLevels)


def checkStreetName(column):
    ## If this is not enough, then we can use real street data
    street = np.asarray(['avenue', 'street', 'st', 'east', 'west', 'north', 'south', 'ave'])
    return generalCheck(column, street)

def checkParkandPlayground(column):
    park = np.asarray(['park','playground','field'])
    return generalCheck(column, park)

def checkCityAgencies(column):
    park = np.asarray(["Actuary, NYC Office of the (NYCOA)","Administrative Justice Coordinator, NYC Office of (AJC)","Administrative Tax Appeals, Office of","Administrative Trials and Hearings, Office of (OATH)","Aging, Department for the (DFTA)","Appointments, Mayor's Office of (MOA)","Brooklyn Public Library (BPL)","Buildings, Department of (DOB)","Business Integrity Commission (BIC)","Campaign Finance Board (CFB)","Center for Innovation through Data Intelligence (CIDI)","Charter Revision Commission","Chief Medical Examiner, NYC Office of (OCME)","Children's Services, Administration for (ACS)","City Clerk, Office of the (CLERK)","City Council, New York","City Planning, Department of (DCP)","City University of New York (CUNY)","Citywide Administrative Services, Department of (DCAS)","Citywide Event Coordination and Management, Office of (CECM)","Civic Engagement Commission (CEC)","Civil Service Commission (CSC)","Civilian Complaint Review Board (CCRB)","Climate Policy & Programs","Commission on Gender Equity (CGE)","Commission to Combat Police Corruption (CCPC)","Community Affairs Unit (CAU)","Community Boards (CB)","Comptroller (COMP)","Conflicts of Interest Board (COIB)","Consumer Affairs, Department of (DCA)","Consumer and Worker Protection, Department of (DCWP)","Contract Services, Mayor's Office of (MOCS)","Correction, Board of (BOC)","Correction, Department of (DOC)","Criminal Justice, Mayor's Office of","Cultural Affairs, Department of (DCLA)","Data Analytics, Mayor's Office of (MODA)","Design and Construction, Department of (DDC)","District Attorney - Bronx County","District Attorney - Kings County (Brooklyn)","District Attorney - New York County (Manhattan)","District Attorney - Queens County","District Attorney - Richmond County (Staten Island)","Education, Department of (DOE)","Elections, Board of (BOE)","Emergency Management, NYC","Environmental Coordination, Mayor’s Office of (MOEC)","Environmental Protection, Department of (DEP)","Equal Employment Practices Commission (EEPC)","Finance, Department of (DOF)","Fire Department, New York City (FDNY)","Fiscal Year 2005 Securitization Corporation","Food Policy Director, Office of the","GreeNYC (GNYC)","Health and Mental Hygiene, Department of (DOHMH)","Homeless Services, Department of (DHS)","Housing Authority, New York City (NYCHA)","Housing Preservation and Development, Department of (HPD)","Housing Recovery Operations (HRO)","Hudson Yards Infrastructure Corporation","Human Resources Administration (HRA)","Human Rights, City Commission on (CCHR)","Immigrant Affairs, Mayor's Office of (MOIA)","Independent Budget Office, NYC (IBO)","Information Privacy, Mayor's Office of (MOIP)","Information Technology and Telecommunications, Department of (DOITT)","Inspector General NYPD, Office of the","Intergovernmental Affairs, Mayor's Office of (MOIGA)","Investigation, Department of (DOI)","Judiciary, Mayor's Advisory Committee on the (MACJ)","Labor Relations, NYC Office of (OLR)","Landmarks Preservation Commission (LPC)","Law Department (LAW)","Library, Brooklyn Public (BPL)","Library, New York Public (NYPL)","Library, Queens Public (QL)","Loft Board (LOFT)","Management and Budget, Office of (OMB)","Mayor's Committee on City Marshals (MCCM)","Mayor's Fund to Advance NYC (Mayor's Fund)","Mayor's Office (OM)","Mayor's Office for Economic Opportunity","Mayor's Office for International Affairs (IA)","Mayor's Office for People with Disabilities (MOPD)","Mayor's Office of Environmental Remediation (OER)","Mayor's Office of Special Projects & Community Events (MOSPCE)","Mayor's Office of the Chief Technology Officer","Mayor’s Office of Minority and Women-Owned Business Enterprises (OMWBE)","Mayor’s Office of Strategic Partnerships (OSP)","Mayor’s Office to End Domestic and Gender-Based Violence (ENDGBV)","Media and Entertainment, Mayor's Office of (MOME)","Media, NYC","NYC & Company (NYCGO)","NYC Children's Cabinet","NYC Cyber Command","NYC Economic Development Corporation (NYCEDC)","NYC Employees' Retirement System (NYCERS)","NYC Health + Hospitals","NYC Service (SERVICE)","NYC Young Men’s Initiative","New York City Transitional Finance Authority (TFA)","New York Public Library (NYPL)","Office of Recovery & Resiliency","Office of ThriveNYC","Office of the Census for NYC","Operations, Mayor's Office of (OPS)","Parks and Recreation, Department of (DPR)","Payroll Administration, Office of (OPA)","Police Department (NYPD)","Police Pension Fund (PPF)","Probation, Department of (DOP)","Procurement Policy Board (PPB)","Property Tax Reform, Advisory Commission on","Public Administrator - Bronx County (BCPA)","Public Administrator - Kings County (KCPA)","Public Administrator - New York County (NYCountyPA)","Public Administrator - Queens County (QPA)","Public Administrator - Richmond County (RCPA)","Public Advocate (PUB ADV)","Public Design Commission","Queens Public Library (QPL)","Records and Information Services, Department of (DORIS)","Rent Guidelines Board (RGB)","Sales Tax Asset Receivable Corporation (STAR)","Sanitation, Department of (DSNY)","School Construction Authority (SCA)","Small Business Services (SBS)","Social Services, Department of (DSS)","Special Commissioner of Investigation for the New York City School District","Special Enforcement, Mayor’s Office of (OSE)","Special Narcotics Prosecutor, NYC Office of the (SNP)","Standards and Appeals, Board of (BSA)","Sustainability, Mayor's Office Of","TSASC, Inc.","Tax Appeals Tribunal, New York City (TAT)","Tax Commission, New York City (TC)","Taxi and Limousine Commission (TLC)","Teachers' Retirement System of the City of New York","Transportation, Department of (DOT)","Veterans' Services, Department of (DVS)","Water Board (NYWB)","Water Finance Authority, NYC Municipal (NYW)","Workforce Development, Mayor's Office of","Youth and Community Development, Department of (DYCD)"])
    return generalCheck(column, park)

def checkBuildingType(column):
    buildingType = np.asarray(['A0	CAPE COD', 'A1	TWO STORIES - DETACHED SM OR MID',
                    'A2	ONE STORY - PERMANENT LIVING QUARTER', 'A3	LARGE SUBURBAN RESIDENCE',
                    'A4	CITY RESIDENCE ONE FAMILY', 'A5	ONE FAMILY ATTACHED OR SEMI-DETACHED',
                    'A6	SUMMER COTTAGE', 'A7	MANSION TYPE OR TOWN HOUSE',
                    'A8	BUNGALOW COLONY - COOPERATIVELY OWNED LAND', 'A9	MISCELLANEOUS ONE FAMILY',
                    'B1	TWO FAMILY BRICK', 'B2	TWO FAMILY FRAME', 'B3	TWO FAMILY CONVERTED FROM ONE FAMILY',
                    'B9	MISCELLANEOUS TWO FAMILY', 'C0	THREE FAMILIES', 'C1	OVER SIX FAMILIES WITHOUT STORES',
                    'C2	FIVE TO SIX FAMILIES', 'C3	FOUR FAMILIES', 'C4	OLD LAW TENEMENT',
                    'C5	CONVERTED DWELLINGS OR ROOMING HOUSE', 'C6	WALK-UP COOPERATIVE',
                    'C7	WALK-UP APT. OVER SIX FAMILIES WITH STORES',
                    'C8	WALK-UP CO-OP; CONVERSION FROM LOFT/WAREHOUSE', 'C9	GARDEN APARTMENTS',
                    'CM	MOBILE HOMES/TRAILER PARKS', 'D0	ELEVATOR CO-OP; CONVERSION FROM LOFT/WAREHOUSE',
                    'D1	ELEVATOR APT; SEMI-FIREPROOF WITHOUT STORES', 'D2	ELEVATOR APT; ARTISTS IN RESIDENCE',
                    'D3	ELEVATOR APT; FIREPROOF WITHOUT STORES', 'D4	ELEVATOR COOPERATIVE',
                    'D5	ELEVATOR APT; CONVERTED', 'D6	ELEVATOR APT; FIREPROOF WITH STORES',
                    'D7	ELEVATOR APT; SEMI-FIREPROOF WITH STORES', 'D8	ELEVATOR APT; LUXURY TYPE',
                    'D9	ELEVATOR APT; MISCELLANEOUS', 'E1	FIREPROOF WAREHOUSE', 'E2	CONTRACTORS WAREHOUSE',
                    'E3	SEMI-FIREPROOF WAREHOUSE', 'E4	METAL FRAME WAREHOUSE', 'E7	SELF-STORAGE WAREHOUSES',
                    'E9	MISCELLANEOUS WAREHOUSE', 'F1	FACTORY; HEAVY MANUFACTURING - FIREPROOF',
                    'F2	FACTORY; SPECIAL CONSTRUCTION - FIREPROOF', 'F4	FACTORY; INDUSTRIAL SEMI-FIREPROOF',
                    'F5	FACTORY; LIGHT MANUFACTURING', 'F8	FACTORY; TANK FARM',
                    'F9	FACTORY; INDUSTRIAL-MISCELLANEOUS', 'G0	GARAGE; RESIDENTIAL TAX CLASS 1',
                    'G1	ALL PARKING GARAGES', 'G2	AUTO BODY/COLLISION OR AUTO REPAIR',
                    'G3	GAS STATION WITH RETAIL STORE', 'G4	GAS STATION WITH SERVICE/AUTO REPAIR',
                    'G5	GAS STATION ONLY WITH/WITHOUT SMALL KIOSK', 'G6	LICENSED PARKING LOT',
                    'G7	UNLICENSED PARKING LOT', 'G8	CAR SALES/RENTAL WITH SHOWROOM',
                    'G9	MISCELLANEOUS GARAGE OR GAS STATION', 'GU	CAR SALES OR RENTAL LOTS WITHOUT SHOWROOM',
                    'GW	CAR WASH OR LUBRITORIUM FACILITY', 'G9	MISCELLANEOUS GARAGE',
                    'HB	BOUTIQUE: 10-100 ROOMS, W/LUXURY FACILITIES, THEMED, STYLISH, W/FULL SVC ACCOMMODATIONS',
                    'HH	HOSTELS- BED RENTALS IN DORMITORY-LIKE SETTINGS W/SHARED ROOMS & BATHROOMS',
                    'HR	SRO- 1 OR 2 PEOPLE HOUSED IN INDIVIDUAL ROOMS IN MULTIPLE DWELLING AFFORDABLE HOUSING',
                    'HS	EXTENDED STAY/SUITE: AMENITIES SIMILAR TO APT; TYPICALLY CHARGE WEEKLY RATES & LESS EXPENSIVE THAN FULL-SERVICE HOTEL',
                    'H1	LUXURY HOTEL', 'H2	FULL SERVICE HOTEL',
                    'H3	LIMITED SERVICE; MANY AFFILIATED WITH NATIONAL CHAIN', 'H4	MOTEL',
                    'H5	HOTEL; PRIVATE CLUB, LUXURY TYPE', 'H6	APARTMENT HOTEL',
                    'H7	APARTMENT HOTEL - COOPERATIVELY OWNED', 'H8	DORMITORY', 'H9	MISCELLANEOUS HOTEL',
                    'I1	HOSPITAL, SANITARIUM, MENTAL INSTITUTION', 'I2	INFIRMARY', 'I3	DISPENSARY',
                    'I4	HOSPITAL; STAFF FACILITY', 'I5	HEALTH CENTER, CHILD CENTER, CLINIC', 'I6	NURSING HOME',
                    'I7	ADULT CARE FACILITY', 'I9	MISCELLANEOUS HOSPITAL, HEALTH CARE FACILITY',
                    'J1	THEATRE; ART TYPE LESS THAN 400 SEATS', 'J2	THEATRE; ART TYPE MORE THAN 400 SEATS',
                    'J3	MOTION PICTURE THEATRE WITH BALCONY', 'J4	LEGITIMATE THEATRE, SOLE USE',
                    'J5	THEATRE IN MIXED-USE BUILDING', 'J6	TELEVISION STUDIO', 'J7	OFF BROADWAY TYPE THEATRE',
                    'J8	MULTIPLEX PICTURE THEATRE', 'J9	MISCELLANEOUS THEATRE', 'K1	ONE STORY RETAIL BUILDING',
                    'K2	MULTI-STORY RETAIL BUILDING (2 OR MORE)', 'K3	MULTI-STORY DEPARTMENT STORE',
                    'K4	PREDOMINANT RETAIL WITH OTHER USES', 'K5	STAND-ALONE FOOD ESTABLISHMENT',
                    'K6	SHOPPING CENTER WITH OR WITHOUT PARKING', 'K7	BANKING FACILITIES WITH OR WITHOUT PARKING',
                    'K8	BIG BOX RETAIL: NOT AFFIXED & STANDING ON OWN LOT W/PARKING, E.G. COSTCO & BJ',
                    'K9	MISCELLANEOUS STORE BUILDING', 'L1	LOFT; OVER 8 STORIES (MID MANH. TYPE)',
                    'L2	LOFT; FIREPROOF AND STORAGE TYPE WITHOUT STORES', 'L3	LOFT; SEMI-FIREPROOF',
                    'L8	LOFT; WITH RETAIL STORES OTHER THAN TYPE ONE', 'L9	MISCELLANEOUS LOFT',
                    'M1	CHURCH, SYNAGOGUE, CHAPEL', 'M2	MISSION HOUSE (NON-RESIDENTIAL)',
                    'M3	PARSONAGE, RECTORY', 'M4	CONVENT', 'M9	MISCELLANEOUS RELIGIOUS FACILITY',
                    'N1	ASYLUM', 'N2	HOME FOR INDIGENT CHILDREN, AGED, HOMELESS', 'N3	ORPHANAGE',
                    'N4	DETENTION HOUSE FOR WAYWARD GIRLS', 'N9	MISCELLANEOUS ASYLUM, HOME',
                    'O1	OFFICE ONLY - 1 STORY', 'O2	OFFICE ONLY 2 - 6 STORIES',
                    'O3	OFFICE ONLY 7 - 19 STORIES', 'O4	OFFICE ONLY WITH OR WITHOUT COMM - 20 STORIES OR MORE',
                    'O5	OFFICE WITH COMM - 1 TO 6 STORIES', 'O6	OFFICE WITH COMM 7 - 19 STORIES',
                    'O7	PROFESSIONAL BUILDINGS/STAND ALONE FUNERAL HOMES',
                    'O8	OFFICE WITH APARTMENTS ONLY (NO COMM)', 'O9	MISCELLANEOUS AND OLD STYLE BANK BLDGS.',
                    'P1	CONCERT HALL', 'P2	LODGE ROOM', 'P3	YWCA, YMCA, YWHA, YMHA, PAL', 'P4	BEACH CLUB',
                    'P5	COMMUNITY CENTER', 'P6	AMUSEMENT PLACE, BATH HOUSE, BOAT HOUSE', 'P7	MUSEUM',
                    'P8	LIBRARY', 'P9	MISCELLANEOUS INDOOR PUBLIC ASSEMBLY', 'Q1	PARKS/RECREATION FACILTY',
                    'Q2	PLAYGROUND', 'Q3	OUTDOOR POOL', 'Q4	BEACH', 'Q5	GOLF COURSE',
                    'Q6	STADIUM, RACE TRACK, BASEBALL FIELD', 'Q7	TENNIS COURT', 'Q8	MARINA, YACHT CLUB',
                    'Q9	MISCELLANEOUS OUTDOOR RECREATIONAL FACILITY', 'RA	CULTURAL, MEDICAL, EDUCATIONAL, ETC.',
                    'RB	OFFICE SPACE', 'RG	INDOOR PARKING', 'RH	HOTEL/BOATEL', 'RK	RETAIL SPACE',
                    'RP	OUTDOOR PARKING', 'RR	CONDOMINIUM RENTALS', 'RS	NON-BUSINESS STORAGE SPACE',
                    'RT	TERRACES/GARDENS/CABANAS', 'RW	WAREHOUSE/FACTORY/INDUSTRIAL',
                    'R0	SPECIAL CONDOMINIUM BILLING LOT', 'R1	CONDO; RESIDENTIAL UNIT IN 2-10 UNIT BLDG.',
                    'R2	CONDO; RESIDENTIAL UNIT IN WALK-UP BLDG.',
                    'R3	CONDO; RESIDENTIAL UNIT IN 1-3 STORY BLDG.',
                    'R4	CONDO; RESIDENTIAL UNIT IN ELEVATOR BLDG.', 'R5	MISCELLANEOUS COMMERCIAL',
                    'R6	CONDO; RESID.UNIT OF 1-3 UNIT BLDG-ORIG CLASS 1',
                    'R7	CONDO; COMML.UNIT OF 1-3 UNIT BLDG-ORIG CLASS 1',
                    'R8	CONDO; COMML.UNIT OF 2-10 UNIT BLDG.', 'R9	CO-OP WITHIN A CONDOMINIUM',
                    'RR	CONDO RENTALS', 'S0	PRIMARILY 1 FAMILY WITH 2 STORES OR OFFICES',
                    'S1	PRIMARILY 1 FAMILY WITH 1 STORE OR OFFICE',
                    'S2	PRIMARILY 2 FAMILY WITH 1 STORE OR OFFICE',
                    'S3	PRIMARILY 3 FAMILY WITH 1 STORE OR OFFICE', 'S4	PRIMARILY 4 FAMILY WITH 1 STORE OROFFICE',
                    'S5	PRIMARILY 5-6 FAMILY WITH 1 STORE OR OFFICE',
                    'S9	SINGLE OR MULTIPLE DWELLING WITH STORES OR OFFICES', 'T1	AIRPORT, AIRFIELD, TERMINAL',
                    'T2	PIER, DOCK, BULKHEAD', 'T9	MISCELLANEOUS TRANSPORTATION FACILITY',
                    'U0	UTILITY COMPANY LAND AND BUILDING', 'U1	BRIDGE, TUNNEL, HIGHWAY',
                    'U2	GAS OR ELECTRIC UTILITY', 'U3	CEILING RAILROAD', 'U4	TELEPHONE UTILITY',
                    'U5	COMMUNICATION FACILITY OTHER THAN TELEPHONE', 'U6	RAILROAD - PRIVATE OWNERSHIP',
                    'U7	TRANSPORTATION - PUBLIC OWNERSHIP', 'U8	REVOCABLE CONSENT',
                    'U9	MISCELLANEOUS UTILITY PROPERTY', 'V0	ZONED RESIDENTIAL; NOT MANHATTAN',
                    'V1	ZONED COMMERCIAL OR MANHATTAN RESIDENTIAL',
                    'V2	ZONED COMMERCIAL ADJACENT TO CLASS 1 DWELLING: NOT MANHATTAN',
                    'V3	ZONED PRIMARILY RESIDENTIAL; NOT MANHATTAN', 'V4	POLICE OR FIRE DEPARTMENT',
                    'V5	SCHOOL SITE OR YARD', 'V6	LIBRARY, HOSPITAL OR MUSEUM',
                    'V7	PORT AUTHORITY OF NEW YORK AND NEW JERSEY', 'V8	NEW YORK STATE OR US GOVERNMENT',
                    'V9	MISCELLANEOUS VACANT LAND', 'W1	PUBLIC ELEMENTARY, JUNIOR OR SENIOR HIGH',
                    'W2	PAROCHIAL SCHOOL, YESHIVA', 'W3	SCHOOL OR ACADEMY', 'W4	TRAINING SCHOOL',
                    'W5	CITY UNIVERSITY', 'W6	OTHER COLLEGE AND UNIVERSITY', 'W7	THEOLOGICAL SEMINARY',
                    'W8	OTHER PRIVATE SCHOOL', 'W9	MISCELLANEOUS EDUCATIONAL FACILITY', 'Y1	FIRE DEPARTMENT',
                    'Y2	POLICE DEPARTMENT', 'Y3	PRISON, JAIL, HOUSE OF DETENTION',
                    'Y4	MILITARY AND NAVAL INSTALLATION', 'Y5	DEPARTMENT OF REAL ESTATE',
                    'Y6	DEPARTMENT OF SANITATION', 'Y7	DEPARTMENT OF PORTS AND TERMINALS',
                    'Y8	DEPARTMENT OF PUBLIC WORKS', 'Y9	DEPARTMENT OF ENVIRONMENTAL PROTECTION',
                    'Z0	TENNIS COURT, POOL, SHED, ETC.', 'Z1	COURT HOUSE', 'Z2	PUBLIC PARKING AREA',
                    'Z3	POST OFFICE', 'Z4	FOREIGN GOVERNMENT', 'Z5	UNITED NATIONS', 'Z7	EASEMENT',
                    'Z8	CEMETERY', 'Z9	OTHER MISCELLANEOUS'])
    return generalCheck(column, buildingType)

     def SemanticCheck(_sc,column):
    #NameCheck SL
    count = 0
    for i in range(0,int(round(1*len(names)))):
        inp = names[random.randint(0,len(names)-1)]
        if m.search_first_name(inp) == False:
            if m.search_last_name(inp) == False:
                print("not a name")
            else: 
                count+=1
                print(inp,m.search_last_name(inp))
        else:
            count+=1
            print(inp,m.search_first_name(inp))
    probability = (count/len(names))*100
    if probability >= 90:
        print("Name Column")
    #Phone Number Check SL
    countrycode = '1'
    n = '15179181419'
    if len(n) == 10:
        num = countrycode+n
    else:
        num = n
    url = 'http://apilayer.net/api/validate?access_key=167e9c0b6bdce3f2e3318195c6211b1b&number='+num+'&country_code=&format=1'
    r = requests.get(url)
    js = r.json()
    if js['valid'] == False:
        print("not real")
    else:
        print("real")

if __name__ == '__main__':
    column = ['facebook', 'google', 'Spotify', 'nyu']
    name = ['Sujay', 'school', 'Wayne']
    street = ['200 schermerhorn st', 'layafayette st', 'boradway']
    print(checkBusinessName(column))
    print(checkBusinessName(name))
    print(checkBusinessName(street))
    print(checkStreetName(street))