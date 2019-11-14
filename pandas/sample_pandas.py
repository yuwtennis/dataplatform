#!/usr/bin/env

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

#
# Print pandas version
#
print(pd.__version__)

#
# Prep Series
#
prefectures = pd.Series(['Tokyo', 'Osaka', 'Aichi'])
populations = pd.Series([13843404, 8824566, 7539185])
incomes     = pd.Series([7122, 2777, 2263])
expenses    = pd.Series([6744, 2758, 2237])

#
# Init DataFrame
#
japan_census = pd.DataFrame({
                    'Prefecture name': prefectures,
                    'Population': populations,
                    'Incomes': incomes,
                    'Expenses': expenses
                  })

#
# Print 1
#
print('Table overview for pandas.')
print(japan_census.head())
print(japan_census.describe())

#
# Plot bar chart and pdf export
#
japan_census.plot.bar(x='Prefecture name', y='Population')
plt.savefig('bar.pdf')

#
# Series Operation
#
japan_census['Population in thousands'] = populations / 1000
japan_census['Sereishitei'] = populations.apply(lambda x: x > 1000000)

print('Basic series operation')
print(japan_census.head())

#
# logargorithm
#
print('Log Arithmetic')
print(np.log(japan_census['Population']))
print(np.log(japan_census['Incomes']))
print(np.log(japan_census['Expenses']))

#
# Excercize
#

# True is Population 10000000< AND Income 3000<
japan_census['Densed_and_Rich'] = (japan_census['Population']>10000000) & japan_census['Incomes'].apply(lambda v: v > 3000)
print(japan_census.head())
