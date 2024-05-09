import pandas as pd

human_products = pd.read_json('https://dati.zva.gov.lv/zalu-registrs/export/HumanProducts.json', encoding='utf-8-sig')
doping_substances = pd.read_csv('https://data.gov.lv/dati/dataset/3635a536-9b05-4a29-8695-31269d30e7b0/resource/ee8f9b14-1eee-494a-b7f5-6777a8232dcb/download')

print(human_products)
print(doping_substances)
