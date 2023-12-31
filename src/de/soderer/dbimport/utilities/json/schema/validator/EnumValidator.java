package de.soderer.dbimport.utilities.json.schema.validator;

import de.soderer.dbimport.utilities.Utilities;
import de.soderer.dbimport.utilities.json.JsonArray;
import de.soderer.dbimport.utilities.json.JsonNode;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.dbimport.utilities.json.schema.JsonSchemaDependencyResolver;

public class EnumValidator extends BaseJsonSchemaValidator {
    public EnumValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
    	super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
    	
    	if (validatorData == null) {
    		throw new JsonSchemaDefinitionError("Enum data is 'null'", jsonSchemaPath);
    	} else if (!(validatorData instanceof JsonArray)) {
    		throw new JsonSchemaDefinitionError("Enum contains a non-JsonArray", jsonSchemaPath);
    	} else if (((JsonArray) validatorData).size() == 0) {
    		throw new JsonSchemaDefinitionError("Enum contains an empty JsonArray", jsonSchemaPath);
    	}
    }
	
	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		for (Object enumObject : ((JsonArray) validatorData)) {
			if (jsonNode.isNull() && enumObject == null) {
				return;
			} else if (enumObject != null && enumObject.equals(jsonNode.getValue())) {
				return;
			}
		}
		throw new JsonSchemaDataValidationError("Enumeration expected one of '" + Utilities.join((JsonArray) validatorData, "', '") + "' but was " + (jsonNode.isSimpleValue() ? "'" + jsonNode.getValue() + "'" : "'" + jsonNode.getJsonDataType() + "'"), jsonPath);
    }
}
