/**********************************************************************************
 * $URL:$
 * $Id:$
 ***********************************************************************************
 *
 * Copyright (c) 2007, 2008 The Sakai Foundation
 *
 * Licensed under the Educational Community License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.osedu.org/licenses/ECL-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **********************************************************************************/

package org.sakaiproject.component.app.scheduler.jobs;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.StatefulJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.sakaiproject.api.app.scheduler.DelayedInvocation;
import org.sakaiproject.api.app.scheduler.ScheduledInvocationCommand;
import org.sakaiproject.component.app.scheduler.DelayedInvocationReader;
import org.sakaiproject.component.cover.ComponentManager;
import org.sakaiproject.db.api.SqlService;
import org.sakaiproject.time.api.Time;
import org.sakaiproject.time.api.TimeService;

public class ScheduledInvocationRunner implements StatefulJob {

	private static final Log LOG = LogFactory.getLog(ScheduledInvocationRunner.class);


	/* (non-Javadoc)
	 * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
	 */
	public void execute(JobExecutionContext arg0) throws JobExecutionException {

		
		SqlService sqlService = ((SqlService) ComponentManager.get("org.sakaiproject.db.api.SqlService"));
		Time now = ((TimeService) ComponentManager.get("org.sakaiproject.time.api.TimeService")).newTime();
		
		String sql = "SELECT INVOCATION_ID, INVOCATION_TIME, COMPONENT, CONTEXT FROM SCHEDULER_DELAYED_INVOCATION WHERE INVOCATION_TIME < ?";

		Object[] fields = new Object[1];

		fields[0] = now;

		LOG.debug("SQL: " + sql + " NOW:" + now);
		List invocations = sqlService.dbRead(sql, fields, new DelayedInvocationReader());

		for (Iterator i = invocations.iterator(); i.hasNext();) {

			DelayedInvocation invocation = (DelayedInvocation) i.next();

			if (invocation != null) {

				LOG.debug("processing invocation: [" + invocation + "]");

				try {
					ScheduledInvocationCommand command = (ScheduledInvocationCommand) ComponentManager.get(invocation.componentId);
					command.execute(invocation.contextId);
				} catch (Exception e) {
					LOG.error("Failed to execute component: [" + invocation.componentId + "]: " + e);
				} finally {
					sql = "DELETE FROM SCHEDULER_DELAYED_INVOCATION WHERE INVOCATION_ID = ?";

					fields[0] = invocation.uuid;

					LOG.debug("SQL: " + sql);
					sqlService.dbWrite(sql, fields);
				}
			}
		}
	}

}
