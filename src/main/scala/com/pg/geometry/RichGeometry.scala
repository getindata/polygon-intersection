package com.pg.geometry

import com.esri.core.geometry.{GeometryEngine, SpatialReference, WktImportFlags, _}

import scala.language.implicitConversions

class RichGeometry(val geometry: Geometry,
                   val csr: SpatialReference = SpatialReference.create(4326)) extends Serializable {

  def intersects(other: RichGeometry): Boolean = {
    !GeometryEngine.intersect(geometry, other.geometry, csr).isEmpty
  }

  def within(other: RichGeometry): Boolean = {
    GeometryEngine.within(geometry, other.geometry, csr)
  }
}

object RichGeometry extends Serializable {

  implicit def createRichGeometry(g: Geometry): RichGeometry = {
    new RichGeometry(g)
  }

  def fromWkt(wkt: String): RichGeometry = {
    createRichGeometry(GeometryEngine.geometryFromWkt(wkt, WktImportFlags.wktImportDefaults, Geometry.Type.Unknown))
  }
}