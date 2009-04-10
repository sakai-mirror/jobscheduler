/**********************************************************************************
 * $URL:$
 * $Id:$
 ***********************************************************************************
 *
 * Copyright (c) 2008 Sakai Foundation
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

package org.sakaiproject.api.app.scheduler;

import org.sakaiproject.time.api.Time;


public interface ScheduledInvocationManager {

	/**
	 * Creates a new delayed invocation and returns the unique id of the created invocation
	 * 
	 * @param time the date and time the method will be invoked
	 * @param componentId the unique name of a bean in the bean factory which implements 
	 * command pattern DelayedInvocationCommand
	 * @param opaqueContext the key which the tool can use to uniquely identify some 
	 * entity when invoked; i.e. the context
	 * @return unique id of a delayed invocation
	 */
	public String createDelayedInvocation(Time time, String componentId, String opaqueContext);

	/**
	 * Remove a future scheduled invocation by its unique id
	 * 
	 * @param uuid unique id of a delayed invocation
	 */
	public void deleteDelayedInvocation(String uuid);

	/**
	 * Remove future scheduled invocations by the component and/or context,
	 * can specify both items, just a component or just a context, or even leave both
	 * as empty strings to remove all future invocations
	 * 
	 * @param componentId the unique name of a bean in the bean factory which implements 
	 * command pattern DelayedInvocationCommand, may be empty string to match any component
	 * @param opaqueContext the key which the tool can use to uniquely identify some 
	 * entity when invoked; i.e. the context, may be empty string to match any context
	 */
	public void deleteDelayedInvocation(String componentId, String opaqueContext);

	/**
	 * Find future scheduled invocations by the component and/or context,
	 * can specify both items, just a component or just a context, or even leave both
	 * as empty strings to find all future invocations
	 * 
	 * @param componentId the unique name of a bean in the bean factory which implements 
	 * command pattern DelayedInvocationCommand, may be empty string to match any component
	 * @param opaqueContext the key which the tool can use to uniquely identify some 
	 * entity when invoked; i.e. the context, may be empty string to match any context
	 * @return an array of {@link DelayedInvocation} objects representing all scheduled
	 * invocations which match the inputs
	 */
	public DelayedInvocation[] findDelayedInvocations(String componentId, String opaqueContext);

}
