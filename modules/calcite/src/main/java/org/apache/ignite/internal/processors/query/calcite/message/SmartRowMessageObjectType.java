package org.apache.ignite.internal.processors.query.calcite.message;

enum SmartRowMessageObjectType {
    Integer();

    public boolean equals(int v) {
        return ordinal() == v;
    }
}
