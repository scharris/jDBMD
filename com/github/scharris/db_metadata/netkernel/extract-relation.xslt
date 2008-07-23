<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="2.0"
				xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
				xmlns:xs="http://www.w3.org/2001/XMLSchema"
				xmlns:l="http://github.com/scharris/db-metadata" exclude-result-prefixes="l xs">
  <xsl:param name="relation"/> <!-- qualified or unqualified table or view name -->
  <xsl:output method="xml" />

  <xsl:template match="/">

	<!-- Build relation id from relation name -->
	<xsl:variable name="rel-id">
	  <xsl:choose>
		<xsl:when test="contains($relation,'.')"> <!-- relation name already qualified with a schema-->
		  <xsl:value-of select="concat('r:',lower-case($relation))"/>
		</xsl:when>
		<xsl:when test="database-metadata/@schema"> <!-- relation name not qualified, but schema name specified in db metadata -->
		  <xsl:value-of select="concat('r:',lower-case(database-metadata/@schema),'.',lower-case($relation))"/>
		</xsl:when>
		<xsl:otherwise> <!-- No schema for relation, we'll look it up unqualified -->
		  <xsl:value-of select="concat('r:',lower-case($relation))"/>
		</xsl:otherwise>
	  </xsl:choose>
	</xsl:variable>
	<xsl:copy-of select="database-metadata/relations/relation[@id=$rel-id]"/>
  </xsl:template>
</xsl:stylesheet>

