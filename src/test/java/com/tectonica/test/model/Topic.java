package com.tectonica.test.model;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Topic implements Serializable
{
	public static enum TopicKind
	{
		AAA, BBB;
	}

	public String topicId;
	public String objId;
	public TopicKind kind;

	public Topic(String topicId, String objId, TopicKind kind)
	{
		this.topicId = topicId;
		this.objId = objId;
		this.kind = kind;
	}

	public static String bundle(String objId, TopicKind kind)
	{
		return objId + "|" + kind.name();
	}

	public String bundle()
	{
		return bundle(objId, kind);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Topic other = (Topic) obj;
		if (kind != other.kind)
			return false;
		if (objId == null)
		{
			if (other.objId != null)
				return false;
		}
		else if (!objId.equals(other.objId))
			return false;
		if (topicId == null)
		{
			if (other.topicId != null)
				return false;
		}
		else if (!topicId.equals(other.topicId))
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		return "Topic [" + topicId + ": " + objId + " [" + kind + "]";
	}
}