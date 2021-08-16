#!pip install xmltodict
import xmltodict, json, io
import fnmatch
import os
import fnmatch
import pandas as pd

dfProdutoNFe = pd.DataFrame(columns=['nrNfe',
                                     'serieNfe',
                                     'nrDtEmis',
                                     'emitCnpj',
                                     'emitNome',
                                     'prod',
                                     'cstPis'])

def ConverterXmlJson(arquivo):
  with open(arquivo, 'r') as myfile:
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
    tipoProduto = fileJson['nfeProc']['NFe']['infNFe']['det']
    if isinstance(tipoProduto,dict):
        pprod = tipoProduto['prod']
        for imp in tipoProduto['imposto']['PIS'].values():
            pcstPis = imp['CST']
        dfProdutoNFe.loc[len(dfProdutoNFe)] = [pnrNfe,
                                          pserieNfe,
                                          pnrDtEmis,
                                          pemitCnpj,
                                          pemitNome,
                                          pprod,
                                          pcstPis]
    else:
        for i in fileJson['nfeProc']['NFe']['infNFe']['det']:
            pprod = i['prod']
        for cst in i['imposto']['PIS'].values():
            pcstPis = cst['CST']
        dfProdutoNFe.loc[len(dfProdutoNFe)] = [pnrNfe,
                                        pserieNfe,
                                        pnrDtEmis,
                                        pemitCnpj,
                                        pemitNome,
                                        pprod,
                                        pcstPis]
    print('Feito!')

path = '/content/'
for file in os.listdir('/content/'):
    if fnmatch.fnmatch(file, '*[0-9].xml'):
        a = (os.path.join(path,file))
        ConverterXmlJson(a)

for file in os.listdir('/content/'):
    if fnmatch.fnmatch(file, '*[0-9].json'):
        a = (os.path.join(path,file))
        transformarDados2(a)
        print(a)
        print('Converdito')

dfProdutoNFe.to_csv(r'NFEs.csv', index = False, header=True)