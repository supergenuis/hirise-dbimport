package de.soderer.dbimport.utilities.json;

/**
 * Interface to us JsonObject and JsonArray as one Type
 */
public interface JsonItem {
	public boolean isJsonObject();
	public boolean isJsonArray();
}
