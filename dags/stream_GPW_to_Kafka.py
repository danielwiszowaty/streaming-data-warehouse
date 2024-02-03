import yfinance as yf
import json

from uuid import uuid4
from pytz import timezone
import logging
from datetime import datetime, timedelta

import pandas as pd

from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

timezone = timezone('Europe/Warsaw')

default_args = {
    'owner': 'danielwiszowaty',
    'depends_on_past': False,
    #'start_date': days_ago(0),
    'email': ['daniwis272@student.polsl.pl'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

companies_GPW = [
'11B.WA', #11 bit studios SA
'3RG.WA', #3R Games SA
'ABE.WA', #AB SA
'ACG.WA', #AC SA
'ACT.WA', #Action SA
'ADV.WA', #Adiuvo Investments SA
'AGO.WA', #Agora SA
'AGT.WA', #Agroton plc.
'ALL.WA', #Ailleron SA
'AWM.WA', #Airway Medix SA
'ALR.WA', #Alior Bank SA
'ALG.WA', #All in! Games SA
'ALE.WA', #Allegro.eu SA
'AAT.WA', #Alta SA
'ALI.WA', #Altus SA
'AMB.WA', #Ambra SA
'AMC.WA', #Amica SA
'EAT.WA', #AmRest Holdings SE
'ANR.WA', #Answear.com SA
'APT.WA', #Apator SA
'APN.WA', #Aplisens SA
'APE.WA', #APS Energia SA
'ARH.WA', #Archicom SA
'ATC.WA', #Arctic Paper SA
'ART.WA', #Artifex Mundi SA
'ASB.WA', #ASBISc Enterprises plc.
'ABS.WA', #Asseco BS SA
'ACP.WA', #Asseco Poland SA
'ASE.WA', #Asseco SEE SA
'AST.WA', #Astarta Holding PLC
'1AT.WA', #Atal SA
'ATD.WA', #Atende SA
'ATP.WA', #Atlanta Poland SA
'ATG.WA', #ATM Grupa SA
'ATR.WA', #Atrem SA
'APR.WA', #Auto Partner SA
'SAN.WA', #Banco Santander SA
'BHW.WA', #Bank Handlowy w Warszawie SA
'MIL.WA', #Bank Millennium SA
'PEO.WA', #Bank Pekao SA
'BBD.WA', #BBI Development SA
'BFT.WA', #Benefit Systems SA
'BST.WA', #Best SA
'BCM.WA', #Betacom SA
'BCS.WA', #Big Cheese Studio SA
'BIP.WA', #Bio Planet SA
'BCX.WA', #Bioceltix SA
'BMX.WA', #BioMaxima SA
'BIO.WA', #Bioton SA
'BLO.WA', #Bloober Team SA
'BNP.WA', #BNP Paribas Bank Polska SA
'BBT.WA', #BoomBit SA
'BRS.WA', #Boryszew SA
'BOS.WA', #BOŚ SA
'BOW.WA', #Bowim SA
'B24.WA', #Brand24 SA
'BAH.WA', #British Automotive Holding SA w upadłości
'BDX.WA', #Budimex SA
'BMC.WA', #Bumech SA
'CPA.WA', #Capital Partners SA
'CAP.WA', #Capitea SA
'CTX.WA', #Captor Therapeutics SA
'CSR.WA', #Caspar AM SA
'CAV.WA', #Cavatina Holding SA
'CCC.WA', #CCC SA
'CDR.WA', #CD Projekt SA
'CDL.WA', #CDRL SA
'CLN.WA', #Celon Pharma SA
'ENE.WA', #Centrum Medyczne Enel-Med SA
'CEZ.WA', #CEZ a.s.
'CFI.WA', #CFI Holding SA
'CIG.WA', #CI Games SE
'CTS.WA', #City Service SE
'CLD.WA', #Cloud Technologies SA
'CLE.WA', #Coal Energy SA
'COG.WA', #Cognor Holding SA
'CLC.WA', #Columbus Energy SA
'CMR.WA', #Comarch SA
'CMP.WA', #Comp SA
'CPL.WA', #Comperia.pl SA
'CPR.WA', #Compremum SA
'CPD.WA', #CPD SA
'OPG.WA', #CPI FIM SA
'CRJ.WA', #Creepy Jar SA
'CRI.WA', #Creotech Instruments SA
'CBF.WA', #Cyber_Folks SA
'CPS.WA', #Cyfrowy Polsat SA
'CZT.WA', #Czerwona Torebka SA
'DAD.WA', #Dadelo SA
'DAT.WA', #DataWalk SA
'DBE.WA', #DB Energy SA
'DCR.WA', #Decora SA
'DEK.WA', #Dekpol SA
'DEL.WA', #Delko SA
'DVL.WA', #Develia SA
'DBC.WA', #Dębica SA
'DGA.WA', #DGA SA
'DIG.WA', #Digital Network SA
'DTR.WA', #Digitree Group SA
'DNP.WA', #Dino Polska SA
'DOM.WA', #Dom Development SA
'DGE.WA', #Drago entertainment SA
'DPL.WA', #Drozapol-Profil SA
'BDZ.WA', #EC Będzin SA
'ECH.WA', #Echo Investment SA
'EDI.WA', #ED Invest SA
'EEX.WA', #Eko Export SA
'ELT.WA', #Elektrotim SA
'EKP.WA', #Elkop SE
'EMC.WA', #EMC Instytut Medyczny SA
'ENA.WA', #Enea SA
'ENG.WA', #Energa SA
'ENP.WA', #Energoaparatura SA
'ENI.WA', #Energoinstal SA
'ENT.WA', #Enter Air SA
'ERB.WA', #Erbud SA
'EAH.WA', #Esotiq&Henderson SA
'EUC.WA', #EuCO SA w restrukturyzacji
'EUR.WA', #Eurocash SA
'EHG.WA', #Eurohold Bulgaria AD
'ETL.WA', #Eurotel SA
'FAB.WA', #Fabrity Holding SA
'FSG.WA', #Fasing SA
'FFI.WA', #Fast Finance SA w restrukturyzacji
'FEE.WA', #Feerum SA
'FRO.WA', #Ferro SA
'FER.WA', #Ferrum SA
'FON.WA', #Fon SE
'FTE.WA', #Forte SA
'GOP.WA', #Games Operators SA
'GIF.WA', #Gaming Factory SA
'GMT.WA', #Genomtec SA
'GTN.WA', #Getin Holding SA
'GIG.WA', #Gi Group Poland SA
'GKI.WA', #GK Immobile SA
'GLC.WA', #Global Cosmed SA
'GOB.WA', #Gobarto SA
'VIN.WA', #GPM Vindexus SA
'GPW.WA', #GPW SA
'GRX.WA', #GreenX Metals Ltd.
'GEA.WA', #Grenevia SA
'GRN.WA', #Grodno SA
'ATT.WA', #Grupa Azoty SA
'KTY.WA', #Grupa Kęty SA
'GPP.WA', #Grupa Pracuj SA
'GTC.WA', #GTC SA
'HRP.WA', #Harper Hygienics SA
'HEL.WA', #Helio SA
'HRS.WA', #Herkules SA
'HMI.WA', #HM Inwest SA
'HUG.WA', #Huuuge, Inc.
'IFI.WA', #iFirma SA
'IMC.WA', #IMC S.A.
'IMS.WA', #IMS SA
'INC.WA', #INC SA
'ING.WA', #ING Bank Śląski SA
'INP.WA', #Inpro SA
'INK.WA', #Instal Kraków SA
'CAR.WA', #Inter Cars SA
'ITB.WA', #Interbud-Lublin SA
'IPO.WA', #Intersport Polska SA
'INL.WA', #Introl SA
'IFC.WA', #Investment Friends Capital SE
'IPE.WA', #Ipopema Securities SA
'IZB.WA', #Izoblok SA
'IZO.WA', #Izolacja Jarocin SA
'IZS.WA', #Izostal SA
'JSW.WA', #Jastrzębska Spółka Węglowa SA
'JWW.WA', #JWW Invest SA
'KCI.WA', #KCI SA
'KER.WA', #Kernel Holding S.A.
'KGH.WA', #KGHM Polska Miedź SA
'KGL.WA', #KGL SA
'KPL.WA', #Kino Polska TV SA
'KGN.WA', #Kogeneracja SA
'KMP.WA', #Kompap SA
'KOM.WA', #Komputronik SA
'KPD.WA', #KPPD SA
'KCH.WA', #Krakchemia SA
'KRI.WA', #Kredyt Inkaso SA
'KRK.WA', #KRKA d.d.
'KRU.WA', #Kruk SA
'KVT.WA', #Krynica Vitamin SA
'KSG.WA', #KSG Agro S.A.
'LAB.WA', #Labo Print SA
'LRK.WA', #Lark.pl SA
'LRQ.WA', #Larq SA
'APL.WA', #LC SA
'LEN.WA', #Lena Lighting SA
'LTX.WA', #Lentex SA
'LES.WA', #Less SA
'LBT.WA', #Libet SA
'LKD.WA', #Lokum Deweloper SA
'LPP.WA', #LPP SA
'LSI.WA', #LSI Software SA
'LBW.WA', #Lubawa SA
'LWB.WA', #LW Bogdanka SA
'MWT.WA', #M.W. Trade SA
'MAB.WA', #Mabion SA
'06N.WA', #Magna Polonia SA
'MAK.WA', #Makarony Polskie SA
'MGT.WA', #Mangata Holding SA
'MBW.WA', #Marie Brizard Wine & Spirits S.A.
'MVP.WA', #Marvipol Development SA
'MXC.WA', #Maxcom SA
'MBK.WA', #mBank SA
'MCI.WA', #MCI Capital ASI SA
'MDI.WA', #MDI Energia SA
'MDG.WA', #Medicalgorithmics SA
'ICE.WA', #Medinice SA
'MNC.WA', #Mennica Polska SA
'MRC.WA', #Mercator Medical SA
'MCR.WA', #Mercor SA
'MEX.WA', #Mex Polska SA
'MFO.WA', #MFO SA
'MLK.WA', #Milkiland Foods PLC
'MIR.WA', #Miraculum SA
'MRB.WA', #Mirbud SA
'MLS.WA', #ML System SA
'MLG.WA', #MLP Group SA
'MBR.WA', #Mo-Bruk SA
'MOJ.WA', #MOJ SA
'MOL.WA', #MOL Rt.
'MOC.WA', #Molecure SA
'MON.WA', #Monnari Trade SA
'MSP.WA', #Mostostal Płock SA
'MSW.WA', #Mostostal Warszawa SA
'MSZ.WA', #Mostostal Zabrze SA
'MOV.WA', #Movie Games SA
'MUR.WA', #Murapol SA
'MZA.WA', #Muza SA
'NNG.WA', #NanoGroup SA
'NEU.WA', #Neuca SA
'NTC.WA', #New Tech Capital SA
'NWG.WA', #Newag SA
'NXG.WA', #Nexity Global SA
'NTU.WA', #Novaturas AB
'NVG.WA', #Novavis Group SA
'NVT.WA', #Novita SA
'PRI.WA', #NPL Nova SA
'NTT.WA', #NTT System SA
'08N.WA', #Octava SA
'ODL.WA', #Odlewnie Polskie SA
'OEX.WA', #OEX SA
'OND.WA', #Onde SA
'FMG.WA', #ONE SA
'ONO.WA', #onesano SA
'OPN.WA', #Oponeo.pl SA
'OPM.WA', #OPTeam SA
'OPL.WA', #Orange Polska SA
'PKN.WA', #Orlen SA
'OBL.WA', #Orzeł Biały SA
'OTS.WA', #OT Logistics SA
'OVO.WA', #Ovostar Union NV
'NVA.WA', #P.A. Nova SA
'PMP.WA', #Pamapol SA
'PAS.WA', #Passus SA
'PAT.WA', #Patentus SA
'PBG.WA', #PBG SA  w restrukturyzacji w likwidacji
'PBF.WA', #PBS Finanse SA
'PCX.WA', #PCC Exol SA
'PCR.WA', #PCC Rokita SA
'PCF.WA', #PCF Group SA
'PBX.WA', #Pekabex SA
'PCO.WA', #Pepco Group NV
'PPS.WA', #Pepees SA
'PGE.WA', #PGE SA
'PGV.WA', #PGF Polska Grupa Fotowoltaiczna SA
'PHR.WA', #Pharmena SA
'PHN.WA', #PHN SA
'PEN.WA', #Photon Energy NV
'HDR.WA', #PHS Hydrotor SA
'PJP.WA', #PJP Makrum SA
'PKO.WA', #PKO BP SA
'PKP.WA', #PKP Cargo SA
'PLW.WA', #PlayWay SA
'PLZ.WA', #Plaza Centers NV
'PGM.WA', #PMPG Polskie Media SA
'PEP.WA', #Polenergia SA
'PCE.WA', #Police SA
'PXM.WA', #Polimex Mostostal SA
'PTG.WA', #Poltreg SA
'PWX.WA', #Polwax SA
'PTH.WA', #Primetech SA
'PRM.WA', #Prochem SA
'PRT.WA', #Protektor SA
'ZAP.WA', #Puławy SA
'PUR.WA', #Pure Biologics SA
'CRM.WA', #PZ Cormay SA
'PZU.WA', #PZU SA
'QNT.WA', #Quantum Software SA
'QRS.WA', #Quercus TFI SA
'RAE.WA', #Raen SA
'RFK.WA', #Rafako SA
'RAF.WA', #Rafamet SA
'RBW.WA', #Rainbow Tours SA
'RNK.WA', #Rank Progress SA
'RWL.WA', #Rawlplug SA
'RDN.WA', #Redan SA
'RNC.WA', #Reino Capital SA
'RLP.WA', #Relpol SA
'RMK.WA', #Remak-Energomontaż SA
'RES.WA', #Resbud SE
'RVU.WA', #Ryvu Therapeutics SA
'SNK.WA', #Sanok Rubber Company SA
'SPL.WA', #Santander Bank Polska SA
'SNW.WA', #Sanwil Holding SA
'STS.WA', #Satis Group SA
'SCP.WA', #Scope Fluidics SA
'SWG.WA', #Seco/Warwick SA
'SEK.WA', #Seko SA
'SEL.WA', #Selena FM SA
'SLV.WA', #Selvita SA
'SEN.WA', #Serinus Energy plc
'SES.WA', #Sescom SA
'SFS.WA', #Sfinks Polska SA
'SHO.WA', #Shoper SA
'SVRS.WA', #Silvair Inc.
'SFG.WA', #Silvano Group AS
'SIM.WA', #SimFabric SA
'SKH.WA', #Skarbiec Holding SA
'SHD.WA', #Soho Development SA
'SOL.WA', #Solar Company SA
'SON.WA', #Sonel SA
'SPH.WA', #Sopharma AD
'SPR.WA', #SpyroSoft SA
'STX.WA', #Stalexport Autostrady SA
'STP.WA', #Stalprodukt SA
'STF.WA', #Stalprofil SA
'SHG.WA', #Starhedge SA
'SNX.WA', #Sunex SA
'SGN.WA', #Sygnity SA
'SNT.WA', #Synektik SA
'SVE.WA', #Synthaverse SA
'SKA.WA', #Śnieżka SA
'TLX.WA', #Talex SA
'TAR.WA', #Tarczyński SA
'TMR.WA', #Tatry Mountain Resorts a.s.
'TPE.WA', #Tauron PE SA
'TBL.WA', #T-Bull SA
'TEN.WA', #Ten Square Games SA
'THG.WA', #TenderHut SA
'TRR.WA', #Termo-Rex SA
'TSG.WA', #Tesgas SA
'TXT.WA', #Text SA
'TIM.WA', #TIM SA
'TOR.WA', #Torpol SA
'TOW.WA', #Tower Investments SA
'TOA.WA', #Toya SA
'TRK.WA', #Trakcja SA
'TRN.WA', #Trans Polonia SA
'TRI.WA', #Triton Development SA
'ULM.WA', #Ulma Construccion Polska SA
'ULG.WA', #Ultimate Games SA
'UNF.WA', #Unfold.vc ASI SA
'UNI.WA', #Unibep SA
'UCG.WA', #UniCredit SpA
'UNT.WA', #Unimot SA
'URT.WA', #Urteste SA
'VRC.WA', #Vercom SA
'VGO.WA', #Vigo Photonics SA
'VTL.WA', #Vistal Gdynia SA
'VVD.WA', #Vivid Games SA
'VOT.WA', #Votum SA
'VOX.WA', #Voxel SA
'VRG.WA', #VRG SA
'WXF.WA', #Warimpex AG
'WAS.WA', #Wasko SA
'WWL.WA', #Wawel SA
'WLT.WA', #Wielton SA
'IBS.WA', #Wise Finance SA
'WTN.WA', #Wittchen SA
'WOJ.WA', #Wojas SA
'WPL.WA', #WP Holding SA
'XPL.WA', #XPlus SA
'XTB.WA', #XTB DM SA
'XTP.WA', #XTPL SA
'YRL.WA', #yarrl SA
'ZMT.WA', #Zamet SA
'ZEP.WA', #ZE PAK SA
'RPC.WA', #ZM Ropczyce SA
'OTM.WA', #ZPC Otmuchów SA
'ZRE.WA', #Zremb-Chojnice SA
'ZUE.WA', #ZUE SA
'ZUK.WA', #ZUK Stąporków SA
]

def transpose_df(df):
    if df is not None:
        try:
            df = df.stack(level=1).rename_axis(['Date', 'Symbol']).drop(['Dividends', 'Stock Splits'], axis=1)

        except Exception as e:
            logging.error(f'An error occured: {e}')

        finally: 
            return df

def filter_df_based_on_time(df, time):
    if df is not None:
            try:
                df = df.loc[df.index.get_level_values(0) == time].reset_index()
                df['Date'] = pd.Series(df['Date'].dt.tz_localize(None), dtype=object)

            except Exception as e:
                logging.error(f'An error occured: {e}')

            finally: 
                return df

def get_time_with_timezone(delta = 16):
    return (datetime.now(timezone) - timedelta(minutes=delta)).replace(second=0, microsecond=0)


def get_stock_data_every_minute(companies, period = '1d', interval = '1m'):
    tickers = yf.Tickers(companies)
    df = transpose_df(tickers.history(period=period, interval=interval))
    
    #Change delta to none for 9-17 data
    time = get_time_with_timezone()
    df = filter_df_based_on_time(df, time)

    if not df.empty:
        return df
    else:
        return None

def send_message(row):
    message = json.dumps(json.loads(row.to_json()), default=str).encode('utf-8')
    producer.send(topic, message)

def stream_data_to_kafka():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    data = get_stock_data_every_minute(companies_GPW)

    if data is not None:
        for _, row in data.iterrows():
            try:
                message = json.dumps(json.loads(row.to_json()), default=str).encode('utf-8')
                producer.send('stock_data', message)

            except Exception as e:
                logging.error(f'An error occured: {e}')
                continue        

def check_GPW_hours():
    now = datetime.now()
    return (now.hour == 9 and now.minute >= 15) or (now.hour > 9 and now.hour < 17) or (now.hour == 17 and now.minute <= 15)
    return True

stream_data_to_kafka()
    
my_dag = DAG('get_data_from_Yahoo_Finance',
    default_args=default_args,
    start_date=datetime(2023, 11, 3),
    #schedule_interval='* 9-17 * * MON-FRI',
    schedule_interval='* * * * *',
    #schedule_interval='@daily',
    description='Get data from Yahoo Finance every minute',
    catchup=False)

check_time_task = ShortCircuitOperator(
    task_id='check_GPW_hours',
    python_callable=check_GPW_hours,
    dag=my_dag
)


stream_data_task = PythonOperator(
    task_id='stream_data_from_api',
    python_callable=stream_data_to_kafka,
    dag=my_dag
)

check_time_task >> stream_data_task