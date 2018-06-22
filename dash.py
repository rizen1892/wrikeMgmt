import requests
import json
import pandas as pd 
import numpy as np 


pillars = ['IEAB2Y3UI4HGWWVD',
'IEAB2Y3UI4HGWW3S',
'IEAB2Y3UI4HGWXCS',
'IEAB2Y3UI4HGWXGB',
'IEAB2Y3UI4HGWYLZ',
'IEAB2Y3UI4HGWXIA',
'IEAB2Y3UI4HGWXOA',
'IEAB2Y3UI4HGWXWZ',
'IEAB2Y3UI4HGWX25']

task_url = "https://www.wrike.com/api/v3/folders/IEAB2Y3UI4HGWWVD/tasks"
folder_url = "https://www.wrike.com/api/v3/folders/IEAB2Y3UI4HGWWVD/folders"

headers = {
    'authorization': "bearer <your token>",
    
    }

task_fields = str(['parentIds','subTaskIds','customFields'])
folder_fields = str(['customFields','customColumnIds'])

response = requests.get(task_url, 
						headers=headers,
						params={'fields':task_fields, 'descendants': True, 'subTasks': True}
						)
# print(response.url)
response_folders = requests.get(folder_url, headers=headers, 
								params={'fields':folder_fields})



data_tasks = response.json()

data_folders = response_folders.json()


df_tasks = pd.DataFrame.from_dict(data_tasks['data'])
df_tasks.to_csv('tasks.csv',index=False)

df_tasks = df_tasks[['id','title','parentIds','subTaskIds']]



# s = df_tasks.apply(lambda x: pd.Series(x['parentIds']),axis=1).stack().reset_index(level=1, drop=True)
# s.name = 'parentIds'
# df_tasks = df_tasks.drop('parentIds', axis=1).join(s)

# s = df_tasks.apply(lambda x: pd.Series(x['subTaskIds']),axis=1).stack().reset_index(level=1, drop=True)
# s.name = 'subTaskIds'
# df_tasks = df_tasks.drop('subTaskIds', axis=1).join(s)



df_tasks = df_tasks.merge(df_tasks, left_on='id', right_on='id', how='left', suffixes=('_tasks','_subtasks'))


# df_tasks = df_tasks.rename(index=str, columns={'id_x':'task_id', 'title_x':'task_title', 'parentIds_x':'task_parentIds', 
# 						'subTaskIds_x':'task_subtaskIds', 'id_y':'subtask_id', 'title_y':'subtask_title',
#        					'parentIds_y':'subtask_parentIds', 'subTaskIds_y':'subtask_subtaskIds'})

# df_tasks = df_tasks[df_tasks['task_parentIds'] != 'IEAB2Y3UI7777777']



df_folders = pd.DataFrame.from_dict(data_folders['data'])
df_folders = df_folders[['id','title','childIds']]



s = df_folders.apply(lambda x: pd.Series(x['childIds']),axis=1).stack().reset_index(level=1, drop=True)
s.name = 'childIds'
df_folders = df_folders.drop('childIds', axis=1).join(s)
df_folders = df_folders.merge(df_folders, left_on='childIds', right_on='id', how='left')
df_folders = df_folders.merge(df_folders, left_on='childIds_y', right_on='id_x', how='inner')

print(df_folders.columns)
df_folders = df_folders.rename(index=str, 
							columns={'id_x_x':'pillar_id', 'title_x_x':'pillar_title', 'childIds_x_x':'pillar_childIds', 
							'id_y_x':'project_id', 'title_y_x':'project_title','childIds_y_x':'project_childIds', 
							'id_x_y':'job_id',
							'title_x_y':'job_title', 'childIds_x_y':'job_childIds'})

df_folders = df_folders[df_folders['pillar_id'].isin(pillars)]
# print(df_folders)

# df_all = df_folders.merge(df_tasks, left_on='job_id', right_on='task_parentIds', how='left')


# print(df_folders)

# df_folders = splitDataFrameList(df_folders, target_column='childIds', separator=',')

# df_folders = df_folders['childIds'].apply(pd.Series)

# df_folders['childIds'] = df_folders['childIds'].replace('[','')
# print(df_folders)

# df_folders = tidy_split(df_folders, column='childIds', sep=';', keep=False)

# print(df)


df_folders.to_csv('folders.csv',index=False)
# df_all.to_csv('all.csv', index=False)

# for x in range(data_len):
# 	print((data['data'][x]))