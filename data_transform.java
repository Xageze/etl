package dataIntegration;


import java.sql.*;
import java.util.Arrays;

import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static com.mongodb.client.model.Filters.eq;
import org.bson.Document;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Titanic {

	public static void main(String[] args) throws SQLException {
		
		Document doc = null;
		// Replace the placeholder with your MongoDB deployment's connection string
        String uri = "mongodb://127.0.0.1/";
        String sqliteURL = "jdbc:sqlite:/C:/Users/futur/Desktop/EPSI/Integration_Donnees/TP_Atelier/database.db";
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("openFoodFactDB");
            MongoCollection<Document> collection = database.getCollection("products");
            try (Connection conn = DriverManager.getConnection(sqliteURL)) {   
            	try (Statement stmt = conn.createStatement()) {
	            // Get a cursor pointing to all documents in the collection
	            MongoCursor<Document> cursor = collection.find().iterator();
	            
	            // Iterate over the cursor
	            while (cursor.hasNext()) {
	                doc = cursor.next();
	                
	                if (doc != null) {
	                    //System.out.println(doc.toJson());
	                } else {
	                    System.out.println("No matching documents found.");
	                }
	                

	                
	                
	                String insertSQL = "INSERT INTO products (product_name, proteins_100g, energy_value, quantity) VALUES (?, ?, ?, ?)";
	                try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
	                	String product_name = doc.getString("product_name");
	                	String quantity = doc.getString("quantity");
	                	
	 
	                    // Access the 'nutriments' object
	                    Document nutriments = (Document) doc.get("nutriments");

	                    // Access the 'proteins_100g' attribute inside the 'nutriments' object
	                    Object proteins_100gObj = nutriments.get("proteins_100g");
	                    String proteins_100gString = "";
	                    
	                    // Check if the value is a Double
	                    if (proteins_100gObj instanceof Double) {
	                        Double proteins_100g = (Double) proteins_100gObj;
	                        proteins_100gString = String.valueOf(proteins_100g);
	                    } 
	                    // Check if the value is an Integer
	                    else if (proteins_100gObj instanceof Integer) {
	                        Integer proteins_100g = (Integer) proteins_100gObj;
	                        proteins_100gString = String.valueOf(proteins_100g);
	                    } 
	                    else {
	                        System.out.println("Invalid value for proteins_100g.");
	                    }
	                    
	                    
	                    // Access the 'dsffsd' attribute inside the 'nutriments' object
	                    Object energy_valueObj = nutriments.get("energy_value");
	                    String energy_valueString = "";
	                    
	                    // Check if the value is a Double
	                    if (energy_valueObj instanceof Double) {
	                        Double energy_value = (Double) energy_valueObj;
	                        energy_valueString = String.valueOf(energy_value);
	                    } 
	                    // Check if the value is an Integer
	                    else if (energy_valueObj instanceof Integer) {
	                        Integer energy_value = (Integer) energy_valueObj;
	                        energy_valueString = String.valueOf(energy_value);
	                    } 
	                    else {
	                        System.out.println("Invalid value for energy_value.");
	                    }

	                    //System.out.println("product_name: " + product_name);
	                	pstmt.setString(1, product_name);
	                	pstmt.setString(2, proteins_100gString);
	                	pstmt.setString(3, energy_valueString);
	                	pstmt.setString(4, quantity);
	                    // Set more parameters as needed
	                    pstmt.executeUpdate();
	                    System.out.println("Document inserted into SQLite.");
	                }
	                
	                
	               
	            }
	            
	            // Close the cursor to release resources
	            cursor.close();
           
                }
            } 
            
        }
		
	/*	
		
		
		
		SparkSession sparkSession = SparkSession.builder().appName("Titanic Data").master("local").getOrCreate();
		//sparkSession.sparkContext().setLogLevel("WARN");	
		
		String dataFile = "C:/Users/futur/git/atelier-integration-donnees/train.csv";
		
		//GOAL : We will be predicting if a passenger survived or not depending on its features.
		
		//Collect data into Spaark
		
		Dataset<Row> df = sparkSession.read()
				.format("csv")
				.option("header","true")
				.option("delimiter", ",")
				.load(dataFile);
		
		//show 20 top results
		df.show();
		
		System.out.println("Nb lignes : "+df.count());
		System.out.println("Colonnes : "+Arrays.toString(df.columns()));
		System.out.println("Types de donnnées : "+Arrays.toString(df.dtypes()));
		
		//Main stats
		df.describe().show();
		
		
		Dataset<Row> dataset2 = df.select(df.col("Survived").cast("float"),
				df.col("Pclass").cast("float"),
				df.col("Sex"),
				df.col("Age").cast("float"),
				df.col("Fare").cast("float"),
				df.col("Embarked")
				);
		//dataset2.show();
		
		//Display all rows with Age is null
		//dataset2.filter("Age is NULL").show();
		
		//Remplacer les '?' par null
		for(String columnName : dataset2.columns()) {
			dataset2 = dataset2.withColumn(columnName,
					functions.when(dataset2.col(columnName).equalTo("?"), null).otherwise(dataset2.col(columnName)));
		}
		//Supprime les lignes contenant des valeurs nulles
		dataset2 = dataset2.na().drop();
		
		//Indexation de la colonne 'Sex'
		StringIndexerModel indexerSex = new StringIndexer()
				.setInputCol("Sex")
				.setOutputCol("Gender")
				.setHandleInvalid("keep")
				.fit(dataset2);
		dataset2 = indexerSex.transform(dataset2);
		
		//Indexation de la colonne "Embarked"
		StringIndexerModel indexerEmbarked = new StringIndexer()
				.setInputCol("Embarked")
				.setOutputCol("Boarded")
				.setHandleInvalid("keep")
				.fit(dataset2);
		dataset2 = indexerEmbarked.transform(dataset2);
		
		//dataset2.show();
		//dataset2.describe().show();
		
		//check for our data types
		//System.out.println(Arrays.toString(dataset2.dtypes()));

		// Drop unnecessary columns
		dataset2 = dataset2.drop("Sex");
		dataset2 = dataset2.drop("Embarked");
		//dataset2.show();
		
		//Create features column
		//Sélectionner les colonnes nécessaires
		String[] requiredFeatures = {"Pclass", "Age", "Fare", "Gender", "Boarded"};
		
		Column[] selectedColumns = new Column[requiredFeatures.length];
		for (int i=0; i<requiredFeatures.length; i++) {
			selectedColumns[i] = dataset2.col(requiredFeatures[i]);
		}
		
		//Utiliser VectorAssembler pour assembler les features
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(requiredFeatures)
				.setOutputCol("features");
		
		//Transformer les données
		Dataset<Row> transformedData = assembler.transform(dataset2);
		
		//Afficher les données transformés
		//transformedData.show();
		
		//Modeling
		//Create two sets : one for training and one for testing
		Dataset<Row>[] split = transformedData.randomSplit(new double[] {0.8, 0.2});
		Dataset<Row> trainingData = split[0];
		Dataset<Row> testData = split[1];
		
		//trainingData.describe().show();
		//testData.describe().show();
		
		//Using Random Forest Classifier
		
		//Initialiser le RandomForestClassifier
		RandomForestClassifier rf = new RandomForestClassifier()
				.setLabelCol("Survived")
				.setFeaturesCol("features")
				.setMaxDepth(5);
		
		//Former le modèle
		RandomForestClassificationModel rfModel = rf.fit(trainingData);
		
		//Afficher les paramètres du modèle
		System.out.println("Random forest model parameters:\n"+rfModel.explainParams());
		
		//This will give use somthing called a transformer
		//And finally, we predict using the test dataset
		Dataset<Row> predictions = rfModel.transform(testData);
		
		predictions.show();
		
		//Evaluate our model
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				.setLabelCol("Survived")
				.setPredictionCol("prediction")
				.setMetricName("accuracy");
		
		//Afficher les paramètres de l'évaluateur
		System.out.println("Evaluator Parameters:\n" + evaluator.explainParams());
		
		//And we get the accuracy we do
		double accuracy = evaluator.evaluate(predictions);
		System.out.println("Test Accuracy = "+accuracy); //0.843
		
		try {
			Thread.sleep(80000000);
		}
		catch(InterruptedException e) {
			e.printStackTrace();
		}
		sparkSession.stop();
		
		
		*/
		
	}
	

}