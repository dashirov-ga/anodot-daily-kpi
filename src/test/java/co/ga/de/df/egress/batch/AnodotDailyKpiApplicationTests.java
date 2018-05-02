/*
 * Copyright 2012-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package co.ga.de.df.egress.batch;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.rule.OutputCapture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AnodotDailyKpiApplicationTests {

	@Rule
	public OutputCapture outputCapture = new OutputCapture();

	@Test
	@Ignore
	public void testDefaultSettings() throws Exception {
		assertEquals(0, SpringApplication.exit(SpringApplication
				.run(AnodotDailyKpiApplication.class)));
		String output = this.outputCapture.toString();
		assertTrue("Wrong output: " + output, output.contains("completed with the following parameters"));
	}

}
