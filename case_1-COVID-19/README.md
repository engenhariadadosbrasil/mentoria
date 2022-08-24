## Exploração

Começaremos explorando os datasets fornecidos pela Organização Mundial da Saúde, utilizando Spark com Python (PySpark) como ferramentas primárias para nossas soluções e pipelines.

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

## Ciclo de Vida da Solução (CVS)

Nossa solução será dividida em 3 etapas essenciais que terão como objetivo mostrar a evolução do nosso pipeline desde as análises mais simples até a solução completa rodando em nuvem. 

1. Etapa I

Neste primeiro estágio, construíremos processos de extração, carregamento e transformação (ETL/ELT) em código puro, utilizando apenas o Spark para transformações pontuais. 

Arquivos CSV serão as saídas finais.


2. Etapa II

Iremos complementar nosso pipeline com Airflow, para realizar incrementos nos dados que faltam e adicionar mais uma camada de negócios com API para obtenção de dados complementares.

Utilizaremos o plugin Compose do Docker para subir nossa stack.

3. Etapa III

Nessa etapa final, acrescentaremos o uso de dashboard para visualizar nossos dados, criar amostragens visuais para as equipes de analistas e tomadores de decisão. Apache Superset e Dash serão nossas escolhas, podendo também alterarmos para outras ferramentas de visualização.

Kafka será utilizado para a parte de streaming para nosso pipeline.

Na etapa anterior utilizamos o plugin Compose do Docker, e nessa etapa final utilizaremos o `minikube` (podendo ser o `k3s` ou `kind` para executar Kubernetes em modo standalone local) para rodar o Kubernetes localmente. Desse modo, toda nossa stack poderá ser executada localmente.

Por fim, através do Terraform, iremos provisionar nossa infraestrutura nos três maiores provedores de nuvem pública (consulte o [manual de configuração]() para ajustes de credenciais, acessos, políticas e etc) e voilá, nossa solução "final" estará disponível para os stakeholders finais poderem apreciar. 

Cada etapa estará contida em sua respectiva `branch`, para exercitarmos nossas habilidades em gestão e versionamento de código/projeto.

