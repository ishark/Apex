/**
 * Copyright (c) 2014 DataTorrent, Inc. All rights reserved.
 */
package com.datatorrent.stram.cli;

import com.datatorrent.stram.client.AppPackage;
import com.datatorrent.stram.client.DTConfiguration;
import java.io.File;
import com.datatorrent.stram.support.StramTestSupport;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class DTCliTest
{
  @Test
  public void testLaunchAppPackagePropertyPrecedence() throws Exception
  {
    String userHome = System.getProperty("user.home");
    System.setProperty("user.home", System.getProperty("user.dir") + "/src/test/resources/testAppPackage");
    File appPackageFile = StramTestSupport.createAppPackageFile(new File("src/test/resources/testAppPackage/testAppPackage.jar"));
    try {
      DTCli cli = new DTCli();
      cli.init();
      // set launch command options
      DTCli.LaunchCommandLineInfo commandLineInfo = DTCli.getLaunchCommandLineInfo(new String[]{"-D", "dt.test.1=launch-define", "-apconf", "my-app-conf1.xml", "-conf", "src/test/resources/testAppPackage/local-conf.xml"});
      // process and look at launch config

      AppPackage ap = new AppPackage(appPackageFile, true);
      DTConfiguration props = cli.getLaunchAppPackageProperties(ap, commandLineInfo, null);
      Assert.assertEquals("launch-define", props.get("dt.test.1"));
      Assert.assertEquals("local-fs-config", props.get("dt.test.2"));
      Assert.assertEquals("app-package-config", props.get("dt.test.3"));
      Assert.assertEquals("user-home-config", props.get("dt.test.4"));
      Assert.assertEquals("package-default", props.get("dt.test.5"));
    }
    finally {
      System.setProperty("user.home", userHome);
      FileUtils.forceDelete(appPackageFile);
    }
  }
}
