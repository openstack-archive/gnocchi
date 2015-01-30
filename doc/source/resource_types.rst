================
 Resource Types
================

Gnocchi offers different resource types to manage your resources. Each resource
type has its specific typed attributes. All resource types are subtype of the
`generic` type.

Immutable attributes are attributes that cannot be modified after the resource
has been created.


generic
=======

+------------+----------------+-----------+
| Attribute  | Type           | Immutable |
+============+================+===========+
| user_id    | UUID           | Yes       |
+------------+----------------+-----------+
| project_id | UUID           | Yes       |
+------------+----------------+-----------+
| started_at | Timestamp      | Yes       |
+------------+----------------+-----------+
| ended_at   | Timestamp      | No        |
+------------+----------------+-----------+
| type       | String         | Yes       |
+------------+----------------+-----------+
| metrics    | {String: UUID} | No        |
+------------+----------------+-----------+


instance
========

+--------------+---------+-----------+
| Attribute    | Type    | Immutable |
+==============+=========+===========+
| flavor_id    | Integer | No        |
+--------------+---------+-----------+
| image_ref    | String  | No        |
+--------------+---------+-----------+
| host         | String  | No        |
+--------------+---------+-----------+
| display_name | String  | No        |
+--------------+---------+-----------+
| server_group | String  | No        |
+--------------+---------+-----------+


swift_account
=============

No specific attributes.


volume
======

+--------------+---------+-----------+
| Attribute    | Type    | Immutable |
+==============+=========+===========+
| display_name | String  | No        |
+--------------+---------+-----------+
