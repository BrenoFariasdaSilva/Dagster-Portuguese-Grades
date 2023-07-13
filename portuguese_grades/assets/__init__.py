# Assets are used to create data pipelines
# Data pipelines are used to read data, process it and return it in a format that can be used by the model
# Note: Create an assets does not mean that the data is read, processed and returned (materialized) which will be stored on databases or cloud storage.
# Think of the materialization as a instance of the asset which would create a snapshot of the data at that point in time
# Assets can be manually materialized (GUI) or automatically materialized (https://docs.dagster.io/_apidocs/ops#dagster.AssetMaterialization)

from dagster import Output, asset, job, op, PathMetadataValue # Import the asset, job and op decorators from the dagster library

import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.svm import SVR
from sklearn.inspection import permutation_importance

from colorama import Style # For coloring the terminal

# Background colors:
class backgroundColors: # Colors for the terminal
	OKCYAN = "\033[96m" # Cyan
	OKGREEN = "\033[92m" # Green
	WARNING = "\033[93m" # Yellow
	FAIL = "\033[91m" # Red

# Function that reads the data from the csv file and stores it in a pandas dataframe
## Panda dataframe is a 2-dimensional labeled data structure with columns of potentially different types, similar to an Excel spreadsheet or SQL table
@asset # Decorator that marks the function as an asset (SDA - Software Defined Asset)
def read_csv_data(): 
	# csv is stores in the data folder that is in the same directory as the assets.py file
	csv_file_path = "portuguese_grades/data/portuguese_grades.csv"
	csv_file = pd.read_csv(csv_file_path) # Read the csv file and store it in a pandas dataframe

	return csv_file # Return the pandas dataframe

# Function that call read_csv_data() function and print dataframe.head() 
@op # Decorator that marks the function as a op
def csv_head():
	# Lendo os datasets
	data = read_csv_data() # Call the read_csv_data() function and store the dataframe in a variable

	# Removendo colunas que são irrelevantes para a predição da nota final dos estudantes
	data.drop(['school','age','Pstatus','Dalc','Walc','goout','romantic','nursery'],axis=1,inplace=True)

	pd.options.display.max_columns = None
	data.head()

	# Estudo de correlação dos dados
	# Possível ver que as notas do Teste G1 e Teste G2 tem alta correlação com a nota final G3.
	plt.figure(figsize=(18,16))
	sns.heatmap(data.corr(numeric_only = True),annot=True)
	
	# plt.show()
	# Save it into a file
	path_corr = "portuguese_grades/processed_data/correlacao_dos_dados.png"
	# plt.savefig(path_corr)
	plt.imshow()
	plt.axis('off')
	plt.tight_layout(pad = 0)
	Output(value=path_corr, metadata={"path": PathMetadataValue(f"file://{path_corr}")})
	
	# context.add_output_metadata({"path": PathMetadataValue(f"file://{path_corr}")})

	male_students=len(data[data['sex']=='M'])
	female_students=len(data[data['sex']=='F'])
	print(f"{backgroundColors.OKCYAN}Nº. de estudantes homens: {backgroundColors.WARNING}{male_students}{Style.RESET_ALL}")
	print(f"{backgroundColors.OKCYAN}Nº. de estudantes mulheres: {backgroundColors.WARNING}{female_students}{Style.RESET_ALL}")
	print(f"--------------------------------------------------------------")

	# Transformando as variáries para 1 e 0 ao invés de variáveis string data
	d = {'yes':1,'no':0}
	data['schoolsup']=data['schoolsup'].map(d)
	data['famsup']=data['famsup'].map(d)
	data['paid']=data['paid'].map(d)
	data['activities']=data['activities'].map(d)
	#data['nursery']=data['nursery'].map(d)
	data['higher']=data['higher'].map(d)
	data['internet']=data['internet'].map(d)
	#data['romantic']=data['romantic'].map(d)
	d={'F':1,'M':0}
	data['sex']=data['sex'].map(d)
	#transformando a variável de trabalho
	d={'teacher':0,'health':1,'services':2,'at_home':3,'other':4}
	data['Mjob']=data['Mjob'].map(d)
	data['Fjob']=data['Fjob'].map(d)

	#transformando a variável reason
	d={'home':0,'reputation':1,'course':2,'other':3}
	data['reason']=data['reason'].map(d)

	#transformando a variável de com quem a criança mora
	d={'mother':0,'father':1,'other':2}
	data['guardian']=data['guardian'].map(d)

	#transformando a variável de área Rural = r e urbana = 1
	d={'R':0,'U':1}
	data['address']=data['address'].map(d)

	#transformando a variável tamanho da Família (l3 = menor = a 3 pessoas) - (GT3 = maior que 3 )
	d={'LE3':0,'GT3':1}
	data['famsize']=data['famsize'].map(d)

	# Construindo o modelo de predição, separando o atributo G3 como a variável alvo. 
	# Portanto, pretende-se prever a nota G3 utilizando todos os outros atributos
	
	x=data.drop('G3',axis=1)
	y=data['G3']

	# Separando o conjunto de treinamento. 75% dos dados usados para treino. 25% dos dados usados para teste.
	X_train,X_test,y_train,y_test=train_test_split(x,y,test_size=0.3,random_state=42)
	y_train

	#Construindo modelo de aprendizagem baseado no modelo SVM
	
	regressor = SVR(kernel = 'rbf')
	#parametros que podem ser testados: C=1, epsilon=10

	#É possível testar outros modelos SVM
	#support=svm.SVC(C=1.0,kernel='linear',gamma='auto',random_state=None)

	#treinando o modelo
	regressor.fit(X_train,y_train)

	#Testando o modelo.
	#The coefficient of determination R2.
	#O melhor valor do Score R2 é 1.0. O valor pode ser negativo porque o modelo pode ser completamente arbitrário e aleatório.
	#Um modelo constante (dummy), que sempre prediz o valor de Y sem métricas, retornaria 0 como score.

	y_pred=regressor.predict(X_test)
	print(f"{backgroundColors.OKCYAN}regressor.score(X_test,y_test): {backgroundColors.WARNING}{regressor.score(X_test,y_test)}{Style.RESET_ALL}")
	print(f"--------------------------------------------------------------")

	#Testando um novo aluno
	novo_aluno = X_test[:1]
	resultado = regressor.predict(novo_aluno)
	print(f"{backgroundColors.OKCYAN}A nota do novo aluno é: {backgroundColors.WARNING}{resultado}{Style.RESET_ALL}")
	print(f"--------------------------------------------------------------")

	#Diferença entre o predito e a verdade. Apresentando o erro.
	result_df = pd.DataFrame([],columns=[])
	result_df['svr_predicted'] = y_pred # SVR
	result_df['true'] = y_test.values
	result_df['svr_error'] = abs(result_df['true'] - result_df['svr_predicted'])
	result_df

	results = permutation_importance(regressor, X_train,y_train)
	# Calcular importancia
	importance = results.importances_mean
	# Apresentar os valores das importancias dos atributos
	for i,v in enumerate(importance):
		print(f"{backgroundColors.OKCYAN}Feature: {backgroundColors.WARNING}{i}{backgroundColors.OKCYAN}, Score: {backgroundColors.WARNING}{v:.5f}{Style.RESET_ALL}")
	print(f"--------------------------------------------------------------")
	# Gerar grafico
	plt.bar([x for x in range(len(importance))], importance)
	# plt.show()
	# Save it into a file
	plt.savefig('portuguese_grades/processed_data/importancias_dos_atributos.png')

# Function that call csv_head() function and execute it in a process
@job
def print_csv_data_job():
	csv_head()

# The execute_in_process() function executes the job in a separate process
result = print_csv_data_job.execute_in_process()