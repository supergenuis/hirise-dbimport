package de.soderer.dbimport.utilities.json.schema.validator;

import java.util.List;

import de.soderer.dbimport.utilities.json.JsonArray;
import de.soderer.dbimport.utilities.json.JsonDataType;
import de.soderer.dbimport.utilities.json.JsonNode;
import de.soderer.dbimport.utilities.json.JsonObject;
import de.soderer.dbimport.utilities.json.schema.JsonSchema;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDependencyResolver;

public class TypeValidator extends BaseJsonSchemaValidator {
    public TypeValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
    	super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
    	
    	if (validatorData == null) {
    		throw new JsonSchemaDefinitionError("Type data is 'null'", jsonSchemaPath);
    	} else if (!(validatorData instanceof String) && !(validatorData instanceof JsonArray)) {
    		throw new JsonSchemaDefinitionError("Type data is not a 'string' or 'array'", jsonSchemaPath);
    	}
    }

	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
    	if (validatorData instanceof JsonArray) {
    		for (Object typeData : ((JsonArray) validatorData)) {
    			if (typeData == null) {
    	    		throw new JsonSchemaDefinitionError("Type data array contains a 'null' item", jsonSchemaPath);
    			} else if (typeData instanceof String) {
	    	    	if ("any".equals((String) typeData)) {
	    	    		return;
	    			} else {
	    	    		JsonDataType jsonDataType;
	    				try {
	    					jsonDataType = JsonDataType.getFromString((String) typeData);
	    				} catch (Exception e) {
	    					throw new JsonSchemaDefinitionError("Invalid JSON data type '" + validatorData + "'", jsonSchemaPath);
	    				}
	    				
	    	    		if (jsonNode.getJsonDataType() == jsonDataType || (jsonDataType == JsonDataType.NUMBER && jsonNode.getJsonDataType() == JsonDataType.INTEGER)) {
	        	    		return;
		    	    	}
	    	    	}
    			} else if (typeData instanceof JsonObject) {
    				List<BaseJsonSchemaValidator> subValidators = JsonSchema.createValidators((JsonObject) typeData, jsonSchemaDependencyResolver, jsonSchemaPath, jsonNode, jsonPath);
    				try {
						for (BaseJsonSchemaValidator subValidator : subValidators) {
							subValidator.validate();
						}
	    				return;
					} catch (JsonSchemaDataValidationError e) {
						// Do nothing, just check the next array item
					}
    			} else {
    				throw new JsonSchemaDefinitionError("Type data array contains a item that is no 'string' and no 'object'", jsonSchemaPath);
    			}
    		}
    		throw new JsonSchemaDataValidationError("Invalid data type '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
    	} else {
			if ("any".equals((String) validatorData)) {
				return;
			} else {
				JsonDataType jsonDataType;
				try {
					jsonDataType = JsonDataType.getFromString((String) validatorData);
				} catch (Exception e) {
					throw new JsonSchemaDefinitionError("Invalid JSON data type '" + validatorData + "'", jsonSchemaPath);
				}
				
				if (jsonNode.getJsonDataType() != jsonDataType && !(jsonDataType == JsonDataType.NUMBER && jsonNode.getJsonDataType() == JsonDataType.INTEGER)) {
					throw new JsonSchemaDataValidationError("Expected data type is '" + (String) validatorData + "' but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
		    	}
			}
		}
	}
}
