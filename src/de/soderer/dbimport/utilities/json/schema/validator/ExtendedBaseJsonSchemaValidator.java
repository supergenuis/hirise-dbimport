package de.soderer.dbimport.utilities.json.schema.validator;

import de.soderer.dbimport.utilities.json.JsonNode;
import de.soderer.dbimport.utilities.json.JsonObject;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDependencyResolver;

public abstract class ExtendedBaseJsonSchemaValidator extends BaseJsonSchemaValidator {
	protected JsonObject parentValidatorData;
	
	protected ExtendedBaseJsonSchemaValidator(JsonObject parentValidatorData, JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
		super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
		
		if (parentValidatorData == null) {
			throw new JsonSchemaDefinitionError("ParentValidatorData is 'null'", jsonSchemaPath);
		} else {
			this.parentValidatorData = parentValidatorData;
		}
	}
}
