package de.soderer.dbimport.utilities.json.schema.validator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import de.soderer.dbimport.utilities.Utilities;
import de.soderer.dbimport.utilities.json.JsonNode;
import de.soderer.dbimport.utilities.json.JsonObject;
import de.soderer.dbimport.utilities.json.schema.JsonSchema;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDependencyResolver;

public class AdditionalPropertiesValidator extends ExtendedBaseJsonSchemaValidator {
	public AdditionalPropertiesValidator(JsonObject parentValidatorData, JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
		super(parentValidatorData, jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
		
		if (validatorData == null) {
			throw new JsonSchemaDefinitionError("AdditionalProperties data is 'null'", jsonSchemaPath);
    	} else if (!(validatorData instanceof Boolean) && !(validatorData instanceof JsonObject)) {
			throw new JsonSchemaDefinitionError("AdditionalProperties data is not a 'boolean' or 'object'", jsonSchemaPath);
    	}
	}
	
	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (!(jsonNode.isJsonObject())) {
			if (!jsonSchemaDependencyResolver.isUseDraftV4Mode()) {
				throw new JsonSchemaDataValidationError("Expected data type 'object' but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
			}
		} else {		
			List<String> additionalPropertyNames = new ArrayList<String>();
			
			for (String checkPropertyKey : ((JsonObject) jsonNode.getValue()).keySet()) {
				if (parentValidatorData.containsPropertyKey("properties")) {
					if (parentValidatorData.get("properties") == null) {
						throw new JsonSchemaDefinitionError("Properties data is 'null'", jsonSchemaPath);
			    	} else if (!(parentValidatorData.get("properties") instanceof JsonObject)) {
						throw new JsonSchemaDefinitionError("Properties data is not a JsonObject", jsonSchemaPath);
			    	} else if (((JsonObject) ((JsonObject) parentValidatorData).get("properties")).containsPropertyKey(checkPropertyKey)) {
			    		continue;
			    	}
				}
	
				boolean isAdditionalPropertyKey = true;
				
				if (parentValidatorData.containsPropertyKey("patternProperties")) {
					if (parentValidatorData.get("patternProperties") == null) {
						throw new JsonSchemaDefinitionError("PatternProperties data is 'null'", jsonSchemaPath);
			    	} else if (!(parentValidatorData.get("patternProperties") instanceof JsonObject)) {
						throw new JsonSchemaDefinitionError("PatternProperties data is not a JsonObject", jsonSchemaPath);
			    	} else {
			    		for (Entry<String, Object> entry : ((JsonObject) parentValidatorData.get("patternProperties")).entrySet()) {
							if (entry.getValue() == null || !(entry.getValue() instanceof JsonObject)) {
								throw new JsonSchemaDefinitionError("PatternProperties data contains a non-JsonObject", jsonSchemaPath);
					    	} else {
								Pattern propertyKeyPattern;
								try {
									propertyKeyPattern = Pattern.compile(entry.getKey());
								} catch (Exception e1) {
									throw new JsonSchemaDefinitionError("PatternProperties data contains invalid RegEx pattern: " + entry.getKey(), jsonSchemaPath);
								}
								
								if (propertyKeyPattern.matcher(checkPropertyKey).find()) {
									isAdditionalPropertyKey = false;
									break;
								}
					    	}
			    		}
			    	}
				}
	
	    		if (isAdditionalPropertyKey) {
	    			additionalPropertyNames.add(checkPropertyKey);
	    		}
			}
			
			if (additionalPropertyNames.size() > 0) {
				if (validatorData instanceof Boolean) {
					if (!(Boolean) validatorData) {
						throw new JsonSchemaDataValidationError("Unexpected property keys found '" + Utilities.join(additionalPropertyNames, "', '") + "'", jsonPath);
					}
				} else if (validatorData instanceof JsonObject) {
					for (String propertyKey : additionalPropertyNames) {
						JsonNode newJsonNode;
						try {
							newJsonNode = new JsonNode(((JsonObject) jsonNode.getValue()).get(propertyKey));
						} catch (Exception e) {
							throw new JsonSchemaDataValidationError("Invalid data type '" + ((JsonObject) jsonNode.getValue()).get(propertyKey).getClass().getSimpleName() + "'", jsonPath + "." + propertyKey);
						}
						List<BaseJsonSchemaValidator> subValidators = JsonSchema.createValidators(((JsonObject) validatorData), jsonSchemaDependencyResolver, jsonSchemaPath, newJsonNode, jsonPath + "." + propertyKey);
						for (BaseJsonSchemaValidator subValidator : subValidators) {
							subValidator.validate();
						}
					}
				}
			}
		}
    }
}
