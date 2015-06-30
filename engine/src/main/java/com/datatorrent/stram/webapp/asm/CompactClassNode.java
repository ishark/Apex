/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram.webapp.asm;

import java.util.List;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.FieldNode;

/**
 * Store class information only needed by app builder
 *
 * @since 2.1
 */
public class CompactClassNode
{
  
  private int access;
  
  private String name;
  
  private List<CompactFieldNode> ports;

  private List<CompactMethodNode> getterMethods;
  
  private List<CompactMethodNode> setterMethods;
  
  private CompactMethodNode initializableConstructor;
  
  private List<CompactClassNode> innerClasses;
  
  private List<String> enumValues;
  
  private ClassSignatureVisitor csv;
  
  public int getAccess()
  {
    return access;
  }

  public void setAccess(int access)
  {
    this.access = access;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public CompactMethodNode getInitializableConstructor()
  {
    return initializableConstructor;
  }

  public void setInitializableConstructor(CompactMethodNode initializableConstructor)
  {
    this.initializableConstructor = initializableConstructor;
  }

  public List<CompactClassNode> getInnerClasses()
  {
    return innerClasses;
  }

  public void setInnerClasses(List<CompactClassNode> innerClasses)
  {
    this.innerClasses = innerClasses;
  }

  public List<String> getEnumValues()
  {
    return enumValues;
  }

  public void setEnumValues(List<String> enumValues)
  {
    this.enumValues = enumValues;
  }

  public boolean isEnum()
  {
    return (access & Opcodes.ACC_ENUM) == Opcodes.ACC_ENUM;
  }

  public List<CompactMethodNode> getGetterMethods()
  {
    return getterMethods;
  }

  public void setGetterMethods(List<CompactMethodNode> getterMethods)
  {
    this.getterMethods = getterMethods;
  }

  public List<CompactMethodNode> getSetterMethods()
  {
    return setterMethods;
  }

  public void setSetterMethods(List<CompactMethodNode> setterMethods)
  {
    this.setterMethods = setterMethods;
  }

  public ClassSignatureVisitor getCsv()
  {
    return csv;
  }

  public void setCsv(ClassSignatureVisitor csv)
  {
    this.csv = csv;
  }

  public List<CompactFieldNode> getPorts() {
    return ports;
  }

  public void setPorts(List<CompactFieldNode> ports) {
    this.ports = ports;
  }
}
