from scripts import (preprocessing_plannedProduction, preprocessing_price_consumption, preprocessing_weatherdata)

df1 = preprocessing_plannedProduction.import_productionData_FR()
df2 = preprocessing_plannedProduction.import_productionData_CZ()
df3 = preprocessing_plannedProduction.import_crossborderData_CZ()
df4 = preprocessing_plannedProduction.import_crossborderData_DE()
df5 = preprocessing_plannedProduction.import_productionData_DE_actual()
df6 = preprocessing_plannedProduction.import_productionData_DE_planned()

df7 = preprocessing_price_consumption.import_consumptionData()
df8 = preprocessing_price_consumption.import_priceData()

df9 = preprocessing_weatherdata.import_weatherData_CZ()
df10 = preprocessing_weatherdata.import_weatherData_FR()
df11 = preprocessing_weatherdata.import_weatherData_DK()
df12 = preprocessing_weatherdata.import_weatherData_DE()

# Duplicates

print('Number of duplicate rows in df1 is: {}'.format(len(df1[df1.duplicated()])))
print('Number of duplicate rows in df2 is: {}'.format(len(df2[df2.duplicated()])))
print('Number of duplicate rows in df3 is: {}'.format(len(df3[df3.duplicated()])))
print('Number of duplicate rows in df4 is: {}'.format(len(df4[df4.duplicated()])))
print('Number of duplicate rows in df5 is: {}'.format(len(df5[df5.duplicated()])))
print('Number of duplicate rows in df6 is: {}'.format(len(df6[df6.duplicated()])))
print('Number of duplicate rows in df7 is: {}'.format(len(df7[df7.duplicated()])))
print('Number of duplicate rows in df8 is: {}'.format(len(df8[df8.duplicated()])))
print('Number of duplicate rows in df9 is: {}'.format(len(df9[df9.duplicated()])))
print('Number of duplicate rows in df10 is: {}'.format(len(df10[df10.duplicated()])))
print('Number of duplicate rows in df11 is: {}'.format(len(df11[df11.duplicated()])))
print('Number of duplicate rows in df12 is: {}'.format(len(df12[df12.duplicated()])))

# Multiple datetime rows

df1['Dummy'] = 1
df2['Dummy'] = 1
df3['Dummy'] = 1
df4['Dummy'] = 1
df5['Dummy'] = 1
df6['Dummy'] = 1
df7['Dummy'] = 1
df8['Dummy'] = 1
df9['Dummy'] = 1
df10['Dummy'] = 1
df11['Dummy'] = 1
df12['Dummy'] = 1

print('Number of multiple datetime rows in df1 is: {}'.format(
    len(df1.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna())))
print('Number of multiple datetime rows in df2 is: {}'.format(
    len(df2.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna())))
print('Number of multiple datetime rows in df3 is: {}'.format(
    len(df3.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna())))
print('Number of multiple datetime rows in df4 is: {}'.format(
    len(df4.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna())))
print('Number of multiple datetime rows in df5 is: {}'.format(
    len(df5.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna())))
print('Number of multiple datetime rows in df6 is: {}'.format(
    len(df6.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna())))
print('Number of multiple datetime rows in df7 is: {}'.format(
    len(df7.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna())))
print('Number of multiple datetime rows in df8 is: {}'.format(
    len(df8.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna())))
print('Number of multiple datetime rows in df9 is: {}'.format(
    len(df9.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna())))
print('Number of multiple datetime rows in df10 is: {}'.format(
    len(df10.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna())))
print('Number of multiple datetime rows in df11 is: {}'.format(
    len(df11.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna())))
print('Number of multiple datetime rows in df12 is: {}'.format(
    len(df12.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna())))

# Handle multiple datetime rows
df1['Dummy'] = 1

if len(df1.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
    df1 = df1.groupby('date').mean().reset_index()

df1.drop('Dummy', axis=1, inplace=True)

# Delete unnecessary rows
df1[df1['date'] >= pd.to_datetime('01-06-2017 00:00:00', format='%d-%m-%Y %H:%M:%S')]

for i in df5.columns[1:-1]:
    print('DE_production_' + i.replace('[MWh]', '_MW_actual'))
