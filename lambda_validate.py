#%%
import os
print("Current dir:", os.getcwd())

#%%
from lambda_function import lambda_handler

res = lambda_handler(None, None)
print(res)

# %%
