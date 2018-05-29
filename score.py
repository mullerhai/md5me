import  numpy as  np
import pandas as pd

from sklearn.pipeline import make_pipeline
PDO=50 #;/*PDO指比率翻番分数，根据实际调整*/

ZICI0=1/15 #;/*ZICI0一般设为1/60。*/

P0=600 #;/*P0指比率为ZICI0的特定点的分值，一般设为600.*/

B=72.1347520444 #;/*B=PDO/LN(2) */
A=404.65547022 #;/*A=P0+B*LN(1/15)

x=0.000008
result = round(int(A - B * np.log(x/(1-x))))

print(result)
