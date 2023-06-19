val df = spark.read.csv("s3://caminho/para/arquivos/csv")

// Bloqueia o recurso usando o DynamoDB Lock Manager
val lockManager = spark.sparkContext.getLockManager(new DynamoDBLockManagerFactory())
lockManager.acquire("meu-recurso")

// Realiza operações no DataFrame
// ...

// Libera o bloqueio do recurso
lockManager.release("meu-recurso")
