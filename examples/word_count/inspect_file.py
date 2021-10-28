# importing library
import pandas as pd

# df = pd.read_csv('./test1.csv')
# text_column = list(df['text'])

# with open('10mb_file.txt', 'w') as f:
#     for item in text_column:
#         f.write(str(item))
# f.close()

# df = pd.read_csv('./test2.csv')
# text_column = list(df['text'])
# with open('10mb_file.txt', 'a') as f:
#     for item in text_column:
#         f.write(str(item))
# f.close()

# Read in the file
with open('10mb_file.txt', 'r') as file:
    filedata = file.read()

# Replace the target string
filedata = filedata.replace('<br />', '\n')
filedata = filedata.replace('"', ' ')

# Write the file out again
with open('10mb_file.txt', 'w') as file:
    file.write(filedata)
