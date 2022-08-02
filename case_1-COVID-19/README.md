## Exploração

Começaremos explorando os datasets fornecidos pela Organização Mundial da Saúde, utilizando Spark com Python (PySpark) como ferramentas primárias para nossas soluções e pipelines.

### Ciclo de Vida da Solução (CVS)

Nossa solução será dividida em 3 etapas essenciais que terão como objetivo mostrar a evolução do nosso pipeline desde as análises mais simples até a solução completa rodando em nuvem. 

1. Estágio I

Neste primeiro estágio


2. Estágio II



3. Estágio III

### Dataset Covid-19

Utilizamos PySpark em container no Docker com volume local para persistência.

Faça clone do repositório da `mentoria`

```
git clone https://github.com/engenhariadadosbrasil/mentoria.git
```

Dentro da pasta `datasets` contendo os arquivos necessários para nosso lab, rode o seguinte comando, que monta um volume para salvar os dados e alterações no notebook na pasta local e aponta para o diretório `work` no sistema de arquivos do container :

```
docker run -p 8888:8888 -v $PWD/datasets:/home/jovyan/work jupyter/pyspark-notebook
```

Após isso, o diretório `datasets` será carregado no ambiente do Jupyter com o kernel PySpark ativado, dentro da pasta `work`.

Dentro do Jupyter notebook, abra um terminal em outra aba, e extraia o arquivo `archive.zip`, que contém os arquivos necessários para realizarmos os trabalhos.

```
unzip archive.zip
```