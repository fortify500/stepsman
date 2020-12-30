#
# Copyright © 2020 stepsman authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

openssl genrsa -out stepsman_tests_ca.key 2048
openssl req -new -key stepsman_tests_ca.key -x509 -days 3650 -out stepsman_tests_ca.crt -subj /C=CN/ST=localhost/O="localhost Ltd"/CN="localhost Root"
openssl genrsa -out stepsman_tests_server.key 2048
openssl req -new -nodes -key stepsman_tests_server.key -out stepsman_tests_server.csr -subj /C=CN/ST=localhost/L=localhost/O="localhost Server"/CN=localhost
openssl x509 -req -in stepsman_tests_server.csr -CA stepsman_tests_ca.crt -CAkey stepsman_tests_ca.key -CAcreateserial -out stepsman_tests_server.crt
openssl genrsa -out stepsman_tests_client.key 2048
openssl req -new -nodes -key stepsman_tests_client.key -out stepsman_tests_client.csr -subj /C=CN/ST=localhost/L=localhost/O="localhost Client"/CN=localhost
openssl x509 -req -in stepsman_tests_client.csr -CA stepsman_tests_ca.crt -CAkey stepsman_tests_ca.key -CAcreateserial -out stepsman_tests_client.crt
rm stepsman_tests_ca.key
rm stepsman_tests_ca.srl
rm stepsman_tests_client.csr
rm stepsman_tests_server.csr