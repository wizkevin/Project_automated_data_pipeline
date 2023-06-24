# spécifier l'image de base à utiliser.
FROM python:3

# Spécifiez le répertoire de travail pour l'application 
WORKDIR /tp_data_pipeline

# Copiez tous les fichiers de l'application dans le répertoire de travail
COPY . .

# Installez les dépendances de l'application en exécutant la commande pip install 
RUN pip install --no-cache-dir -r requirements.txt

# Exposez le port utilisé par l'application
EXPOSE 3000

# commande pour démarrer l'application
CMD [ "python", "api.py" ]