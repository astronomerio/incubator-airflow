/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, {
  useState, useEffect, useCallback, ReactNode, ReactElement,
} from 'react';
import axios from 'axios';
import { useQueryClient } from 'react-query';

import {
  checkExpire, clearAuth, get, set,
} from 'utils/localStorage';
import { AuthContext } from './context';

type Props = {
  children: ReactNode;
};

const AuthProvider = ({ children }: Props): ReactElement => {
  const [hasValidAuthToken, setHasValidAuthToken] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const queryClient = useQueryClient();

  const clearData = useCallback(() => {
    setHasValidAuthToken(false);
    clearAuth();
    queryClient.clear();
    axios.defaults.headers.common.Authorization = null;
  }, [queryClient]);

  const logout = () => clearData();

  // intercept responses and logout on unauthorized error
  axios.interceptors.response.use(
    (res) => res,
    (err) => {
      if (err && err.response && err.response.status === 401) {
        logout();
      }
      return Promise.reject(err);
    },
  );

  useEffect(() => {
    const getAuthInfo = async () => {
      const authInfo = await axios.get(`${process.env.WEBSERVER_URL}/api/v1/auth-info`);
      console.log(authInfo);
    };
    getAuthInfo();
  }, []);

  useEffect(() => {
    const token = get('token');
    const isExpired = checkExpire('token');
    if (token && !isExpired) {
      axios.defaults.headers.common.Authorization = token;
      setHasValidAuthToken(true);
    } else if (token) {
      clearData();
      setError(new Error('Token invalid, please reauthenticate.'));
    } else {
      setHasValidAuthToken(false);
    }
    setLoading(false);
  }, [clearData]);

  // Login with basic auth.
  // There is no actual auth endpoint yet, so we check against a generic endpoint
  const login = async (username: string, password: string) => {
    setLoading(true);
    setError(null);
    try {
      const authResponse = await axios.post(`${process.env.WEBSERVER_URL}/api/v1/auth/login`, {
        password,
        username,
      });
      setLoading(false);
      if ((authResponse as any).token) {
        set('token', (authResponse as any).token);
        axios.defaults.headers.common.Authorization = `Bearer ${(authResponse as any).token}`;
        setHasValidAuthToken(true);
      } else {
        const error = new Error('Something went wrong, please try again');
        setError(error);
      }
    } catch (e) {
      setLoading(false);
      setError(e);
    }
  };

  return (
    <AuthContext.Provider
      value={{
        hasValidAuthToken,
        logout,
        login,
        loading,
        error,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};

export default AuthProvider;
