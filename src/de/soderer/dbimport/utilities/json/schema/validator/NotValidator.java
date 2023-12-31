package de.soderer.dbimport.utilities.json.schema.validator;

import java.util.List;

import de.soderer.dbimport.utilities.json.JsonNode;
import de.soderer.dbimport.utilities.json.JsonObject;
import de.soderer.dbimport.utilities.json.schema.JsonSchema;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDependencyResolver;

public class NotValidator extends BaseJsonSchemaValidator {
	private List<BaseJsonSchemaValidator> subValidators = null;
	
    public NotValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
    	super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
    	
    	if (validatorData == null) {
    		throw new JsonSchemaDefinitionError("Not-validation data is 'null'", jsonSchemaPath);
    	} else if (validatorData instanceof JsonObject) {
    		subValidators = JsonSchema.createValidators((JsonObject) validatorData, jsonSchemaDependencyResolver, jsonSchemaPath, jsonNode, jsonPath);
    		if (subValidators == null || subValidators.size() == 0) {
    			throw new JsonSchemaDefinitionError("Not-validation JsonObject is empty", jsonSchemaPath);
    		}
    	} else {
    		throw new JsonSchemaDefinitionError("Not-validation property does not have an JsonObject value", jsonSchemaPath);
    	}
    }

	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		boolean didNotApply = false;
		try {
			for (BaseJsonSchemaValidator subValidator : subValidators) {
				subValidator.validate();
			}
		} catch (JsonSchemaDataValidationError e) {
			didNotApply = true;
		}
		if (!didNotApply) {
			throw new JsonSchemaDataValidationError("The 'not' property did apply to JsonNode", jsonPath);
		}
	}
}
