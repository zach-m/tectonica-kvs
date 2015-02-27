package com.tectonica.test.model;

import java.io.Serializable;
import java.util.Date;

@SuppressWarnings("serial")
public class Stopover implements Serializable
{
	public String stpid;
	public Date arrivalTime;
	public Date eta;
	public Date notBefore;

	public Stopover()
	{}

	public Stopover(String stpid, Date arrivalTime, Date eta, Date notBefore)
	{
		this.stpid = stpid;
		this.arrivalTime = arrivalTime;
		this.eta = eta;
		this.notBefore = notBefore;
	}

	@Override
	public String toString()
	{
		return "[" + stpid + "]";
	}

	@Override
	public boolean equals(Object obj)
	{
		return stpid.equals(((Stopover) obj).stpid);
	}
}