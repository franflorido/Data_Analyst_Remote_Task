import numpy as np
import pandas as pd
import re
from geopy.geocoders import Nominatim
from countryinfo import CountryInfo

#################################################################################################################
# ----------------------------------------------- INPUT DATA ---------------------------------------------------#
#################################################################################################################

############################ Please change the import and export paths if required ##############################

# Path where the excel is going to be exported to
output_path = "D:/Documentos/Data_Analyst_Remote_Task/results.xlsx"

# Path of the input files
input_path = 'D:/Documentos/Data_Analyst_Remote_Task/supplier_car.json'
guideline_path = 'D:/Documentos/Data_Analyst_Remote_Task/Target_Data.xlsx'

# Normalization dictionaries 
condition_normalization_dict = {'New':0,
                                'Neu':0,
                                'Original Condition':1,
                                'Originalzustand':1,
                                'Used with guarantee':2,
                                'Gebraucht mit Garantie':2,
                                'Used':3,
                                'Gebraucht':3,
                                'Restored':4,
                                'Restauriert':4,
                                'Restoration Project':5,
                                'Restaurierungsprojekt':5,
                                'Antique':6,
                                'Oldtimer':6,
                                'demonstration model':7,
                                'Vorführmodell':7,
                                'Occasion':8}

type_normalization_dict = {'car':0,
                           'Wagen':0,
                           'motorcycle':1,
                           'motorrad':1}


#################################################################################################################
# --------------------------------------------------- CODE -----------------------------------------------------#
#################################################################################################################


def load_data(input_path, guideline_path):
    '''
        Function to load the data from the input path
    '''
    
    # Load data
    data_df = pd.read_json(input_path, lines=True)
    target_df = pd.read_excel(guideline_path)
    
    return data_df, target_df


def data_preprocesing(data_df):
    '''
        Function to for data preprocessing
    '''

    # First preparation of the dataset 
    data_df = data_df.sort_values(by=['ID']).reset_index().drop(columns=['index'])
    data_df.rename(columns = {'Attribute Names':'Attribute_Names', 'Attribute Values':'Attribute_Values'}, inplace = True)

    # Extracting attributes from Attribute Names column and removing duplicates
    attributes_df = data_df.pivot(index='ID', columns='Attribute_Names', values='Attribute_Values')
    data_df = pd.merge(data_df, attributes_df, how='inner', left_on = 'ID', right_on = 'ID')
    data_df.set_index(['ID'])
    data_df_1 = data_df.drop(columns=['Attribute_Names', 'Attribute_Values'])
    data_df_1 = data_df_1.drop_duplicates(subset='ID', keep='first')

    # Removing the part of ModelText column to ModelTypeText to obtain desired attribute
    data_df_1['ModelTypeText'] = data_df_1['ModelTypeText'].str.upper()
    data_df_1['ModelTypeText'] = [e.replace(k, '') for e, k in zip(data_df_1.ModelTypeText.astype('str'), data_df_1.ModelText.astype('str'))]

    # Obtaining Country, currency and zip for each one of the cities in the dataset
    city_list = data_df.City.unique().tolist()
    geolocator = Nominatim(user_agent = "geoapiExercises")
    country_list = []
    currency_list = []
    zip_list = []
    for city in city_list:
        location = geolocator.geocode(city)
        country_name = location[0].split(",")[-1]
        country_name = country_name.split("/")[1]
        currencies = CountryInfo(country_name).currencies()[1]
        zip_e = CountryInfo(country_name).iso()['alpha3']
        country = CountryInfo(country_name).iso()['alpha2']
        country_list.append(country)
        currency_list.append(currencies)
        zip_list.append(zip_e)

    city_country_dict = dict(zip(city_list, country_list))
    city_currency_dict = dict(zip(city_list, currency_list))
    city_zip_dict = dict(zip(city_list, zip_list))
    data_df_1['country']= data_df_1['City'].map(city_country_dict)
    data_df_1['zip']= data_df_1['City'].map(city_zip_dict)
    data_df_1['currency']= data_df_1['City'].map(city_currency_dict)

    # type column creation
    data_df_1['type'] = np.where(~data_df_1['BodyTypeText'].isnull(),'car','null')

    # Editing ConsumptionTotalText column to get the desired format
    for i in data_df_1.index:
        s = data_df_1.loc[i,'ConsumptionTotalText']
        x = s.split('/')
        if (s is not None) & (len(x) > 1) :
            # split string by "/" and keep only letters
            r1 = re.sub(r'[^a-zA-Z]', '', x[0])
            r2 = re.sub(r'[^a-zA-Z]', '', x[1])
            data_df_1.loc[i,'ConsumptionTotalText'] = r1+'_'+r2+'_consumption'
       
    # Fix to include ml and km into mileage_unit and mileage
    columns = data_df_1.columns.tolist()
    if ('ml' not in columns) & ('Km' in columns):
        data_df_1['mileage_unit'] = 'kilometer'
    elif ('ml' in columns) & ('Km' not in columns):
        data_df_1['mileage_unit'] = 'mile'
    else:
        data_df_1['mileage_unit'] = 'kilometer'
        mask = data_df_1['Km'].isnull()
        data_df_1.loc[mask, 'mileage_unit'] = 'mile'
        value = data_df_1.loc[mask, 'ml']
        data_df_1.loc[mask, 'Km'] = value

    # Asigning the desired data type to the columns
    data_df_1['Km'] = data_df_1['Km'].astype(float)
    data_df_1['FirstRegMonth'] = data_df_1['FirstRegMonth'].astype(float)
    data_df_1['FirstRegYear'] = data_df_1['FirstRegYear'].astype('int64')

    return data_df_1

def data_normalization(dic_1,dic_2,data_df_2):
    '''
        Function to normalize data
    '''
    
    # Normalize columns
    data_df = data_df_2.copy()
    data_df['ConditionTypeText']= data_df['ConditionTypeText'].map(dic_1)
    data_df['type']= data_df['type'].map(dic_2)

    return data_df

def data_integration(data_df_2, target_df):
    '''
        Function to perform data integration
    '''

    # Renaming columns that dont match target data
    data_df_2 = data_df_2.rename(columns={'BodyTypeText':'carType','BodyColorText':'color','ConditionTypeText':'condition',
                                          'City': 'city', 'MakeText': 'make', 'FirstRegYear':'manufacture_year', 'Km':'mileage',
                                          'ModelTypeText': 'model_variant', 'ModelText': 'model','FirstRegMonth': 'manufacture_month',
                                          'ConsumptionTotalText': 'fuel_consumption_unit'})
    
    # Creating columns mising as null 
    baseline_columns = target_df.columns.unique().tolist()
    for i in baseline_columns:
        if i not in data_df_2.columns.unique().tolist():
            if i == 'price_on_request':
                data_df_2[i] = False
            else:
                data_df_2[i] = 'null'

    # Retriving only the needed columns 
    data_df_2 = data_df_2[baseline_columns]

    # Changing NaN for null
    data_df_2 = data_df_2.fillna('null')

    return data_df_2

def export_to_excel(data_df, data_df_2, data_df_3, path):
    '''
        Function to export data to excel file
    '''

    # create a excel writer object
    with pd.ExcelWriter(path, engine='xlsxwriter') as writer:

        data_df.to_excel(writer, sheet_name="Pre-processing", index=False)
        data_df_2.to_excel(writer, sheet_name="Normalization", index=False)
        data_df_3.to_excel(writer, sheet_name="Integration", index=False)

    print("Output file saved at: " + str(path))

if __name__ == "__main__":

    data_df, target_df = load_data(input_path, guideline_path)
    data_preprocesed = data_preprocesing(data_df)
    data_normalized = data_normalization(condition_normalization_dict, type_normalization_dict, data_preprocesed)
    final_dataset = data_integration(data_normalized, target_df)
    export_to_excel(data_preprocesed, data_normalized, final_dataset, output_path)






