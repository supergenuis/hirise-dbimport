package de.soderer.dbimport.utilities.json.schema.validator;

import java.util.ArrayList;
import java.util.List;

import de.soderer.dbimport.utilities.json.JsonArray;
import de.soderer.dbimport.utilities.json.JsonNode;
import de.soderer.dbimport.utilities.json.JsonObject;
import de.soderer.dbimport.utilities.json.schema.JsonSchema;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDependencyResolver;

public class AnyOfValidator extends BaseJsonSchemaValidator {
	private List<List<BaseJsonSchemaValidator>> subValidatorPackages = null;
	
    public AnyOfValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
    	super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
    	
    	if (validatorData == null) {
    		throw new JsonSchemaDefinitionError("AnyOf array is 'null'", jsonSchemaPath);
    	} else if (validatorData instanceof JsonArray) {
    		subValidatorPackages = new ArrayList<List<BaseJsonSchemaValidator>>();
    		for (int i = 0; i < ((JsonArray) validatorData).size(); i++) {
    			Object subValidationData = ((JsonArray) validatorData).get(i);
    			if (subValidationData instanceof JsonObject) {
    				subValidatorPackages.add(JsonSchema.createValidators((JsonObject) subValidationData, jsonSchemaDependencyResolver, jsonSchemaPath, jsonNode, jsonPath));
    			} else {
    				throw new JsonSchemaDefinitionError("AnyOf array contains a non-JsonObject", jsonSchemaPath);
    			}
    		}
    		if (subValidatorPackages == null || subValidatorPackages.size() == 0) {
    			throw new JsonSchemaDefinitionError("AnyOf array is empty", jsonSchemaPath);
    		}
    	} else {
    		throw new JsonSchemaDefinitionError("AnyOf property does not have an array value", jsonSchemaPath);
    	}
    }

	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		for (List<BaseJsonSchemaValidator> subValidatorPackage : subValidatorPackages) {
			try {
				for (BaseJsonSchemaValidator subValidator : subValidatorPackage) {
					subValidator.validate();
				}
				return;
			} catch (JsonSchemaDataValidationError e) {
			}
		}
		throw new JsonSchemaDataValidationError("No option of 'anyOf' property did apply to JsonNode", jsonPath);
	}
}
