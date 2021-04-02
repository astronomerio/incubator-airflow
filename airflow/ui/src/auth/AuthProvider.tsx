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
import axios, { AxiosResponse } from 'axios';
import { useQueryClient } from 'react-query';

import type { User } from 'interfaces';
import {
  clearAuth, get, set,
} from 'utils/localStorage';
import { AuthContext } from './context';

type Props = {
  children: ReactNode;
};

interface RefreshResponse extends AxiosResponse { accessToken?: string }

interface AuthResponse extends AxiosResponse {
  user?: User;
  refreshToken?: string;
  token?: string
}

const AuthProvider = ({ children }: Props): ReactElement => {
  const [user, setUser] = useState<User | undefined>();
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const queryClient = useQueryClient();

  const clearData = useCallback(() => {
    setUser(undefined);
    clearAuth();
    queryClient.clear();
    axios.defaults.headers.common.Authorization = null;
  }, [queryClient]);

  const logout = () => clearData();

  /*
    * If it is an unauthorized error we will try once to refresh the accessToken,
    * then on success, redo the original request. If not, logout.
  */
  axios.interceptors.response.use(
    (res) => res,
    async (err) => {
      const originalRequest = err.config;
      if (err.response && err.response.status) {
        if (err.response.status === 401 && originalRequest.url === '/refresh') {
          logout();
          return Promise.reject(err);
        }

        if (err.response.status === 401 && !originalRequest.retry && originalRequest.url !== '/auth/login') {
          originalRequest.retry = true;
          const auth = JSON.parse(get('auth'));
          const { accessToken }: RefreshResponse = await axios.post('/refresh', null, { headers: { Authorization: `Bearer ${auth.refreshToken}` } });
          if (accessToken) {
            set('auth', JSON.stringify({
              ...auth,
              token: accessToken,
            }));
            axios.defaults.headers.common.Authorization = `Bearer ${accessToken}`;
            return axios(originalRequest);
          }
        }
      }
      return Promise.reject(err);
    },
  );

  useEffect(() => {
    let auth;
    try {
      auth = JSON.parse(get('auth'));
    } catch (e) {
      clearData();
    }
    if (auth && auth.token) {
      axios.defaults.headers.common.Authorization = `Bearer ${auth.token}`;
      setUser(auth.user);
    } else {
      clearData();
    }
    setLoading(false);
  }, [clearData]);

  // Login with basic auth.
  const login = async (username: string, password: string) => {
    setLoading(true);
    setError(null);
    try {
      const authResponse: AuthResponse = await axios.post(`${process.env.WEBSERVER_URL}/api/v1/auth/login`, {
        password,
        username,
      });
      setLoading(false);
      if (authResponse.token) {
        set('auth', JSON.stringify(authResponse));
        axios.defaults.headers.common.Authorization = `Bearer ${authResponse.token}`;
        setUser(authResponse.user);
      } else {
        setError(new Error('Something went wrong, please try again'));
      }
    } catch (e) {
      setLoading(false);
      setError(e);
    }
  };

  return (
    <AuthContext.Provider
      value={{
        user,
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
