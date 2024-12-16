pipeline {
    agent any

    environment {
        DOCKERHUB_REPO = 'jjesus2000/crypto'
        DOCKERHUB_CREDENTIALS = 'creds'
    }

    stages {
        stage('Clone Repository') {
            steps {
                git 'https://github.com/jonyjesus18/crypto.git'
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    docker.build("${DOCKERHUB_REPO}:latest")
                }
            }
        }

        stage('Push to Docker Hub') {
            steps {
                script {
                    docker.withRegistry('https://registry.hub.docker.com', DOCKERHUB_CREDENTIALS) {
                        docker.image("${DOCKERHUB_REPO}:latest").push()
                    }
                }
            }
        }

        stage('Deploy Latest Image') {
            steps {
                script {
                    // Stop and remove the old container if it exists
                    sh """
                    docker ps -q --filter "name=python-app-container" | xargs -r docker stop
                    docker ps -aq --filter "name=python-app-container" | xargs -r docker rm
                    """

                    // Pull the latest image and run the container
                    sh """
                    docker pull ${DOCKERHUB_REPO}:latest
                    docker run -d --name python-app-container -p 5000:5000 ${DOCKERHUB_REPO}:latest
                    """
                }
            }
        }
    }
}
