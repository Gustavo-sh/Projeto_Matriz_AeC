# Projeto_Matriz_AeC
Projeto que visa automatizar o processo de Matriz, de modo que as áreas tenham maior autonomia na geração da mesma, e reduzir a quantidade de reuniões, sendo elas necessárias apenas quando houver desacordo por parte de alguma das áreas de apoio.

# Uso
Rode o comando pip install -r requirements.txt na raiz do projeto para instalar as dependencias do mesmo. 
Após instalar as dependencias, rode o comando uvicorn main:app --reload na raiz para rodar localmente, ou rode uvicorn main:app --host 0.0.0.0 --port 80 --workers 4 para que outras máquinas consigam acessar o site através do IP da sua máquina. 