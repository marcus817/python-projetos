import os, json, glob, xmltodict, fnmatch
import pandas as pd

dfProdutoNFe = pd.DataFrame(columns=['nrNfe',
                                     'serieNfe',
                                     'nrDtEmis',
                                     'emitCnpj',
                                     'emitNome',
                                     'destCnpj',
                                     'destNome',
                                     'Codprod',
                                     'Nomeprod',
                                     'NCMprod',
                                     'CFOPprod',
                                     'VUniprod',
                                     'pVProdprod',
                                     'cstPis'])

def limparDiretorio():
  json_files = glob.glob('*.json')

  for json_file in json_files:
    try:
        os.remove(json_file)
    except OSError as e:
        print(f"Error:{ e.strerror}")
  print("Diretorio Limpo")

def diretorioAtual():
    folder = os.getcwd()
    print(f'Diretório atual é: {folder}')

def ConverterXmlJson(arquivo):
#Inserir o econding para ler os arquivos XML
  with open(arquivo, 'r',encoding='utf-8') as myfile:
   obj = xmltodict.parse(myfile.read())
   print(arquivo)
   if (eval(json.dumps(obj['nfeProc']['NFe']['infNFe']['@Id']))):
    nameFile = eval(json.dumps(obj['nfeProc']['NFe']['infNFe']['@Id']))
    out_file = open(nameFile+".json", "w")     
    json.dump(obj, out_file, indent = 6)    
    out_file.close()
   else:
     print("Arquivo Invalido") 


def transformarDados2(arquivo):
    f = open(arquivo)
    fileJson = json.load(f)
    pnrNfe = fileJson['nfeProc']['NFe']['infNFe']['ide']['nNF']
    pserieNfe = fileJson['nfeProc']['NFe']['infNFe']['ide']['serie']
    pnrDtEmis = fileJson['nfeProc']['NFe']['infNFe']['ide']['dhEmi']
    pemitCnpj = fileJson['nfeProc']['NFe']['infNFe']['emit']['CNPJ']
    pemitNome = fileJson['nfeProc']['NFe']['infNFe']['emit']['xNome']
    pdestCNPJ = fileJson['nfeProc']['NFe']['infNFe']['dest']['CNPJ']
    pdestNome = fileJson['nfeProc']['NFe']['infNFe']['dest']['xNome']
    tipoProduto = fileJson['nfeProc']['NFe']['infNFe']['det']
    if isinstance(tipoProduto,dict):
        pCodprod = tipoProduto['prod']['cProd']
        pNomeprod = tipoProduto['prod']['xProd']
        pNCMprod = tipoProduto['prod']['NCM']
        pCFOPprod = tipoProduto['prod']['CFOP']
        pVUniprod = tipoProduto['prod']['vUnCom']
        pVProdprod = tipoProduto['prod']['vProd']


        for imp in tipoProduto['imposto']['PIS'].values():
            pcstPis = imp['CST']
        dfProdutoNFe.loc[len(dfProdutoNFe)] = [pnrNfe,
                                          pserieNfe,
                                          pnrDtEmis,
                                          pemitCnpj,
                                          pemitNome,
                                          pdestCNPJ,
                                          pdestNome,
                                          pCodprod,
                                          pNomeprod,
                                          pNCMprod,
                                          pCFOPprod,
                                          pVUniprod,
                                          pVProdprod,
                                          pcstPis]
    else:
        for tipoProduto in fileJson['nfeProc']['NFe']['infNFe']['det']:
            pCodprod = tipoProduto['prod']['cProd']
            pNomeprod = tipoProduto['prod']['xProd']
            pNCMprod = tipoProduto['prod']['NCM']
            pCFOPprod = tipoProduto['prod']['CFOP']
            pVUniprod = tipoProduto['prod']['vUnCom']
            pVProdprod = tipoProduto['prod']['vProd']
            for cst in tipoProduto['imposto']['PIS'].values():
                 pcstPis = cst['CST']
            dfProdutoNFe.loc[len(dfProdutoNFe)] = [pnrNfe,
                                          pserieNfe,
                                          pnrDtEmis,
                                          pemitCnpj,
                                          pemitNome,
                                          pdestCNPJ,
                                          pdestNome,
                                          pCodprod,
                                          pNomeprod,
                                          pNCMprod,
                                          pCFOPprod,
                                          pVUniprod,
                                          pVProdprod,
                                          pcstPis]
    print('Feito!')


os.chdir('C:\\Users\\marcu\\Desktop\\Fiscal\\XML\\NFe')
folder = os.getcwd()

limparDiretorio()

for file in os.listdir(os.getcwd()):
    if fnmatch.fnmatch(file, '*[0-9].xml'):
        a = (os.path.join(folder,file))
        ConverterXmlJson(a)
        print(f'Diretório atual é: {folder}')

for file in os.listdir(os.getcwd()):
    if fnmatch.fnmatch(file, '*[0-9].json'):
        a = (os.path.join(folder,file))
        transformarDados2(a)
        print(a)
        print('Convertido')
        
limparDiretorio()

dfProdutoNFe.to_csv(r'NFEs.csv', index = False, header=True)