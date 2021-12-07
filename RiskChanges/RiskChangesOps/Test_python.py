import pandas as pd
print("hello world")
d = {'col1': [1, 2], 'col2': [3, 4]}
df = pd.DataFrame(data=d)
df=df.rename(columns={'col1':'ashok'})
print(df)
