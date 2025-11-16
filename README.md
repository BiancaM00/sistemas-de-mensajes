Sistema de Mensajes – Proyecto Final
Este proyecto implementa un sistema completo para la gestión, procesamiento y almacenamiento de datos meteorológicos utilizando Docker, Kafka, Python (FastAPI/consumidores/ productores)y PostgreSQL.  
La arquitectura permite recibir datos desde un productor, procesarlos mediante un consumidor y almacenarlos de forma segura en una base de datos.  
Arquitectura del Proyecto
El proyecto está compuesto por los siguientes servicios:

1. Producer (Python)
Envia periódicamente datos meteorológicos ficticios al topic de Kafka.  
Ejemplo de datos enviados:
- Temperatura  
- Humedad  
- Velocidad del viento  
- Fecha y hora de la medición  

2. Consumer (Python)
Escucha el topic de Kafka y almacena los registros recibidos en la base de datos PostgreSQL.
3. PostgreSQL
Base de datos que guarda todos los logs meteorológicos.  
Al iniciar, se ejecuta **init.sql**, que crea la tabla necesaria para almacenar los datos.
4. Kafka + Zookeeper
Sistema de mensajería que permite la comunicación asíncrona entre el productor y el consumidor.
5. Monitoring
Carpeta destinada para futuras integraciones como Grafana o Prometheus.
Estructura del Proyecto
sistema de mensajes/
│
├── arquitectura/
│   └── architecture_visual.png
│
├── consumidor/
│   ├── Dockerfile
│   ├── consumer.py
│   └── requirements.txt
│
├── productor/
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
│
├── README.md
├── docker-compose.yml
└── init_db.sql
