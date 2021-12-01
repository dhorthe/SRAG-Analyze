from numpy import NAN, integer as np
import numpy
import pandas as pd
import connection
from tqdm import tqdm

TabelaDados = pd.read_csv("dados2.csv", sep=";")

TabelaDados["DT_NASC"] = TabelaDados['DT_NASC'].astype('datetime64')
TabelaDados["TP_IDADE"] = pd.to_numeric(TabelaDados['TP_IDADE'], errors='coerce')
TabelaDados["CS_RACA"] = pd.to_numeric(TabelaDados['CS_RACA'], errors='coerce')
#TabelaDados["CS_RACA"] = TabelaDados['CS_RACA'].astype('object')
TabelaDados["MAE_VAC"] = TabelaDados['MAE_VAC'].astype('object')
TabelaDados["DT_DOSEUNI"] = pd.to_datetime(TabelaDados['DT_DOSEUNI'], errors='coerce')
#TabelaDados["DT_DOSEUNI"] = TabelaDados['DT_DOSEUNI'].astype('object')
TabelaDados["M_AMAMENTA"] = TabelaDados['M_AMAMENTA'].astype('object')
TabelaDados["DT_1_DOSE"] = pd.to_datetime(TabelaDados['DT_1_DOSE'], errors='coerce')
#TabelaDados["DT_1_DOSE"] = TabelaDados['DT_1_DOSE'].astype('object')
TabelaDados["PCR_FLUASU"] = TabelaDados['PCR_FLUASU'].astype('object')
TabelaDados["PCR_FLUBLI"] = TabelaDados['PCR_FLUBLI'].astype('object')
TabelaDados["PCR_OUTRO"] = TabelaDados['PCR_OUTRO'].astype('object')
TabelaDados["HISTO_VGM"] = TabelaDados['HISTO_VGM'].astype('object')
TabelaDados["CO_PS_VGM"] = TabelaDados['CO_PS_VGM'].astype('object')
TabelaDados["LO_PS_VGM"] = TabelaDados['LO_PS_VGM'].astype('object')
TabelaDados["DT_VGM"] = TabelaDados['DT_VGM'].astype('object')
TabelaDados["PAC_DSCBO"] = TabelaDados['PAC_DSCBO'].astype('object')
TabelaDados["RES_IGM"] = pd.to_numeric(TabelaDados['RES_IGM'], errors='coerce')
TabelaDados["DT_RES"] = pd.to_datetime(TabelaDados['DT_RES'], errors='coerce')
TabelaDados["RES_IGA"] = pd.to_numeric(TabelaDados['RES_IGA'], errors='coerce')

TabelaDados["OUT_SOR"] = pd.to_datetime(TabelaDados['OUT_SOR'], errors='coerce')
TabelaDados["TP_SOR"] = pd.to_numeric(TabelaDados['TP_SOR'], errors='coerce')
TabelaDados["RES_IGA"] = pd.to_datetime(TabelaDados['RES_IGA'], errors='coerce')
TabelaDados["RES_IGA"] = pd.to_datetime(TabelaDados['RES_IGA'], errors='coerce')
TabelaDados["RES_IGA"] = pd.to_datetime(TabelaDados['RES_IGA'], errors='coerce')
TabelaDados["RES_IGA"] = pd.to_datetime(TabelaDados['RES_IGA'], errors='coerce')



print(TabelaDados['DT_NASC'].isna().sum())

def ConsultIdResid():
    conn = connection.InitConnection()
    cursor = conn.cursor()
    cursor.execute("SELECT id_resid from residencia ORDER BY id_resid DESC LIMIT 1")
    id_resid = cursor.fetchall()
    cursor.close()
    conn.close()
    return id_resid

#metodo para encontrar a posição e retornar o valor dela na lista dados 
'''def FindPosition(coluna):
    cod=0
    for item in range(len(dados)):
        #print(item, coluna)
        if dados[item] == coluna:
            cod+=1
            #print(dados[cod])
            return cod
        cod+=1'''

#metodo para inserir dados na tabela municipio, regional_saude e residencia
def InsertResidencia(TabelaDados, linha):
    #print(sg_uf, id_rg_resi, co_rg_resi, id_mn_resi, co_mun_res, id_pais, co_pais, cs_zona)
    conn = connection.InitConnection()
    cursor = conn.cursor()
    
    #insert na tabela residencia
  
    cursor.execute("INSERT INTO residencia (id_pais, co_pais, cs_zona, fk_cod_mun_ibge, fk_cod_regional) values ('{0}', {1}, {2}, {3}, {4})".format(TabelaDados['ID_PAIS'][linha], TabelaDados['CO_PAIS'][linha], TabelaDados['CS_ZONA'][linha], TabelaDados['CO_MUN_RES'][linha], TabelaDados['CO_RG_RESI'][linha]))
    conn.commit()
    cursor.close()
    conn.close()

def InsertMunicipio(TabelaDados, linha):
    conn = connection.InitConnection()
    cursor = conn.cursor()
    #insert na tabela municipio
    try:
        print(TabelaDados['CO_MUN_RES'][linha], TabelaDados['ID_MN_RESI'][linha], TabelaDados['SG_UF'][linha])
        cursor.execute("INSERT INTO municipio (cod_ibge, id_mun, uf) values ({0}, '{1}', '{2}')".format(TabelaDados['CO_MUN_RES'][linha], TabelaDados['ID_MN_RESI'][linha], TabelaDados['SG_UF'][linha]))
        conn.commit()
    except:
        conn.rollback()           
        print("dado já existe na tabela municipio")
    
    cursor.close()
    conn.close() 
    
def InsertRegional(TabelaDados, linha):    
    conn = connection.InitConnection()
    cursor = conn.cursor()
    #insert na tabela regional_saude
    try:        
        cursor.execute("INSERT INTO regional_saude (cod_regional, id_regional) values ({0}, '{1}')".format(TabelaDados['CO_RG_RESI'][linha], TabelaDados['ID_RG_RESI'][linha]))
        conn.commit()
    except:
        conn.rollback()
        print("dado já existe na tabela regional_saude")    
    cursor.close()
    conn.close() 
    
def InsertUnidade(TabelaDados, linha):
    conn = connection.InitConnection()
    cursor = conn.cursor()
    try:
        #insert na tabela unidade_saude
        cursor.execute("INSERT INTO unidade_saude (cod_uni_saude, id_unidade) values ({0}, '{1}')".format(TabelaDados['CO_UNI_NOT'][linha], TabelaDados['ID_UNIDADE'][linha]))
        conn.commit()
    except:
        conn.rollback()
        #print("dado já existe na tabela unidade_saude")
    cursor.close()
    conn.close()

def InsertPaciente(TabelaDados, linha):
    #para cada atributo que você quer valorar, é preciso procura-lo utilizando o FindPosition('nome coluna') 
    conn = connection.InitConnection()
    cursor = conn.cursor()
    fk_id_resid = ConsultIdResid()
    fk_id_resid = fk_id_resid[0][0]
    
    #insert na tabela paciente
    cursor.execute("INSERT INTO paciente (cs_sexo, dt_nasc, nu_idade_n, tp_idade, cod_idade, cs_gestant, cs_raca, cs_etinia, cs_escol_n, pac_cocbo, pac_dscbo, fk_id_resid)"+
    "values ( '{0}' , '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, '{9}', '{10}',{11})".format(TabelaDados['CS_SEXO'][linha], TabelaDados['DT_NASC'][linha], TabelaDados['NU_IDADE_N'][linha], TabelaDados['TP_IDADE'][linha], TabelaDados['COD_IDADE'][linha], TabelaDados['CS_GESTANT'][linha], TabelaDados['CS_RACA'][linha], TabelaDados['CS_ETINIA'][linha], TabelaDados['CS_ESCOL_N'][linha], TabelaDados['PAC_COCBO'][linha], TabelaDados['PAC_DSCBO'][linha], fk_id_resid))
    conn.commit()
    cursor.close()
    connection.CloseConnection(conn)
    conn.close()

NameCollums = ['DT_NOTIFIC','SEM_NOT', 'DT_SIN_PRI','SEM_PRI',	'SG_UF_NOT',	'ID_REGIONA',	'CO_REGIONA',	'ID_MUNICIP',	'CO_MUN_NOT',	'ID_UNIDADE',	'CO_UNI_NOT',	'CS_SEXO',	'DT_NASC',	'NU_IDADE_N',	'TP_IDADE',	'COD_IDADE',	'CS_GESTANT',	'CS_RACA',	'CS_ETINIA',	'CS_ESCOL_N',	'ID_PAIS',	'CO_PAIS',	'SG_UF',	'ID_RG_RESI',	'CO_RG_RESI',	'ID_MN_RESI',	'CO_MUN_RES',	'CS_ZONA',	'SURTO_SG',	'NOSOCOMIAL',	'AVE_SUINO',	'FEBRE',	'TOSSE',	'GARGANTA',	'DISPNEIA',	'DESC_RESP',	'SATURACAO',	'DIARREIA',	'VOMITO',	'OUTRO_SIN',	'OUTRO_DES',	'PUERPERA',	'FATOR_RISC',	'CARDIOPATI',	'HEMATOLOGI',	'SIND_DOWN',	'HEPATICA',	'ASMA',	'DIABETES',	'NEUROLOGIC',	'PNEUMOPATI',	'IMUNODEPRE',	'RENAL',	'OBESIDADE',	'OBES_IMC',	'OUT_MORBI',	'MORB_DESC',	'VACINA',	'DT_UT_DOSE',	'MAE_VAC',	'DT_VAC_MAE',	'M_AMAMENTA',	'DT_DOSEUNI',	'DT_1_DOSE','DT_2_DOSE','ANTIVIRAL','TP_ANTIVIR','OUT_ANTIV','DT_ANTIVIR','HOSPITAL','DT_INTERNA','SG_UF_INTE','ID_RG_INTE','CO_RG_INTE',"ID_MN_INTE",    "CO_MU_INTE",    "UTI",    "DT_ENTUTI",  "DT_SAIDUTI",    "SUPORT_VEN",    "RAIOX_RES",    "RAIOX_OUT",    "DT_RAIOX",  "AMOSTRA",    "DT_COLETA",    "TP_AMOSTRA",   "OUT_AMOST",    "PCR_RESUL",    "DT_PCR",    "POS_PCRFLU",    "TP_FLU_PCR",   "PCR_FLUASU",   "FLUASU_OUT",    "PCR_FLUBLI",    "FLUBLI_OUT",    "POS_PCROUT",    "PCR_VSR",    "PCR_PARA1",    "PCR_PARA2",   "PCR_PARA3",    "PCR_PARA4", "PCR_ADENO",   "PCR_METAP",    "PCR_BOCA",    "PCR_RINO",    "PCR_OUTRO",    "DS_PCR_OUT",   "CLASSI_FIN",    "CLASSI_OUT",  "CRITERIO",    "EVOLUCAO",    "DT_EVOLUCA",    "DT_ENCERRA",  "DT_DIGITA",   "HISTO_VGM",   "PAIS_VGM",    "CO_PS_VGM",    "LO_PS_VGM",    "DT_VGM",    "DT_RT_VGM",    "PCR_SARS2",    "PAC_COCBO",    "PAC_DSCBO",  "OUT_ANIM",    "DOR_ABD",  "FADIGA", "PERD_OLFT",    "PERD_PALA",  "TOMO_RES",  "TOMO_OUT",  "DT_TOMO",    "TP_TES_AN",  "DT_RES_AN", "RES_AN",  "POS_AN_FLU",    "TP_FLU_AN",    "POS_AN_OUT",    "AN_SARS2",    "AN_VSR",    "AN_PARA1",    "AN_PARA2",    "AN_PARA3",    "AN_ADENO",    "AN_OUTRO",  "DS_AN_OUT",    "TP_AM_SOR",    "SOR_OUT",    "DT_CO_SOR", "TP_SOR", "OUT_SOR",  "DT_RES", "RES_IGG",    "RES_IGM",  "RES_IGA"]        
#dados é a lista onde estão armazenados os nomes das colunas e os seus valores, respectivamente
#ou seja, quando você quer pegar um valor da coluna SEM_NOT, você precisa do item dados[(posicao=nomeColuna)+1]
#
for linha in tqdm(range(1)):
    
    for index in range(154):
        print(TabelaDados[NameCollums[index]],TabelaDados[NameCollums[index]].isna().sum())
    #InsertMunicipio(TabelaDados, linha)
    #print(NameCollums[index])
    #print(TabelaDados[NameCollums[index]])        
    #InsertMunicipio(TabelaDados, linha)
    #InsertRegional(TabelaDados, linha)
    #InsertUnidade(TabelaDados, linha)       
    #InsertResidencia(TabelaDados, linha)
    #InsertPaciente(TabelaDados, linha)
